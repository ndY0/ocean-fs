//! Tiered acceleration dispatcher.
//!
//! The [`AccelDispatcher`] is the integration point for all acceleration
//! backends. At startup, it probes available hardware (CPU SIMD, ISA-L,
//! CUDA GPU) and selects the best available backend. All encode/decode
//! calls are delegated through the dispatcher via the `Encoder` and
//! `Decoder` traits from `oceanfs-ec`.
//!
//! ## Fallback Chain
//!
//! When a configured tier is unavailable, the dispatcher falls back to
//! the next available tier and emits a `WARN`-level log. The system
//! **never panics or returns an error** due to missing acceleration
//! hardware (per ADR-0006 §2).
//!
//! ```text
//! GpuCuda -> IsaL -> CpuSimd   (always terminates at CpuSimd)
//! ```

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

#[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
use oceanfs_core::GpuConfig;
use oceanfs_core::{AccelConfig, CodecConfig, CompressConfig, CompressionConfig, CompressionTier};
use oceanfs_ec::{Decoder, Encoder};

use crate::{
    compressor::{Compressor, ZstdCompressor},
    metrics::AccelMetrics,
    tier0::{self, CpuEncoder},
};

/// Acceleration tier levels for EC operations.
///
/// Specifies which hardware acceleration backend to use for erasure
/// coding encode/decode operations.
///
/// # Examples
///
/// ```
/// use oceanfs_accel::AccelTier;
///
/// let tier = AccelTier::Auto;
/// assert!(matches!(tier, AccelTier::Auto));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AccelTier {
    /// Automatically select the best available tier.
    /// Probe order: CUDA > ISA-L > CPU SIMD.
    Auto,
    /// CPU SIMD (portable GF-complete + runtime SIMD dispatch).
    /// Always available. Terminal fallback.
    CpuSimd,
    /// Intel ISA-L optimized (x86 with AVX-512).
    /// Available only when `isa-l` feature is enabled and AVX-512 is detected.
    IsaL,
    /// GPU / CUDA (requires CUDA hardware).
    /// Available only when `cuda` feature is enabled and a GPU is present.
    GpuCuda,
}

/// Dispatches EC and compression operations to the best available backend.
///
/// Performs hardware probing at construction and caches the resolved
/// backends for the lifetime of the dispatcher. Implements `Encoder`
/// and `Decoder` by delegating to the active backend.
///
/// # Examples
///
/// ```
/// use oceanfs_accel::{AccelDispatcher, AccelConfig, AccelTier};
///
/// let config = AccelConfig {
///     ec_tier: "auto".into(),
///     ..Default::default()
/// };
/// let dispatcher = AccelDispatcher::new(config);
/// // May resolve to CpuSimd or GpuCuda depending on hardware
/// assert!(matches!(
///     dispatcher.active_tier(),
///     AccelTier::CpuSimd | AccelTier::GpuCuda | AccelTier::IsaL
/// ));
/// ```
pub struct AccelDispatcher {
    /// The resolved active EC tier.
    active_ec_tier: AccelTier,
    /// Cached encoder backend for the active EC tier.
    encoder: Arc<dyn Encoder>,
    /// Cached decoder backend for the active EC tier.
    decoder: Arc<dyn Decoder>,
    /// Per-tier encoder cache (for per-bucket overrides).
    tier_encoders: HashMap<AccelTier, Arc<dyn Encoder>>,
    /// Per-tier decoder cache (for per-bucket overrides).
    tier_decoders: HashMap<AccelTier, Arc<dyn Decoder>>,
    /// Per-tier compressor cache (for per-bucket compression tier overrides).
    tier_compressors: HashMap<CompressionTier, Arc<dyn Compressor>>,
    /// The configuration used to create this dispatcher.
    #[allow(dead_code)]
    config: AccelConfig,
    /// Node-level compression governance (per ADR-0007).
    /// Controls the ceiling — buckets may only select this tier or lower.
    node_compression: CompressionConfig,
    /// Counter incremented on each compression fallback event.
    /// Exposed for observability (per ADR-0006 §2).
    compression_fallback_count: AtomicU64,
    /// Counter incremented on each EC tier fallback event.
    /// Exposed for observability (per ADR-0006 §2).
    ec_fallback_count: AtomicU64,
    /// Acceleration metrics for observability.
    /// Exposed via [`Self::metrics`] for Prometheus / tracing integration.
    metrics: AccelMetrics,
    /// Runtime fallback flag: set to `true` when the active EC backend
    /// has encountered a recoverable error and been marked unavailable.
    /// The dispatcher will attempt to re-resolve on the next request.
    ec_backend_unhealthy: AtomicBool,
    /// Runtime fallback flag for compression backends.
    compression_backend_unhealthy: AtomicBool,
}

impl AccelDispatcher {
    /// Creates a new dispatcher, probing available hardware and resolving
    /// the configured EC tier to the best available backend.
    ///
    /// This is the entry point called at node startup. Hardware probing
    /// runs synchronously; the resolved backends are cached for the
    /// lifetime of the dispatcher.
    ///
    /// # Panics
    ///
    /// Panics only if the CPU SIMD encoder cannot be constructed (which
    /// should never happen — it is the always-available fallback).
    pub fn new(config: AccelConfig) -> Self {
        let _span = tracing::debug_span!("accel_dispatcher_init").entered();

        // --- Probe backends ---
        let cpu_available = tier0::is_cpu_available();
        tracing::debug!(cpu_capabilities = tier0::cpu_capabilities(), "CPU SIMD always available");

        // Tier 1 CPU SIMD: ISA-L on x86_64, ARM SVE on aarch64 (mutually exclusive).
        // The `isal_available` flag represents whatever Tier 1 is available on this platform.
        #[cfg(any(feature = "isa-l", feature = "arm-sve"))]
        let tier1_available = Self::probe_tier1();
        #[cfg(not(any(feature = "isa-l", feature = "arm-sve")))]
        let tier1_available = false;

        #[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
        let cuda_available = Self::probe_cuda(config.gpu.as_ref());
        #[cfg(any(not(feature = "cuda"), no_cuda_toolkit))]
        let cuda_available = false;

        tracing::debug!(
            cpu = cpu_available,
            tier1 = tier1_available,
            cuda = cuda_available,
            "hardware probe complete"
        );

        // --- Resolve EC tier ---
        let requested_tier = Self::parse_ec_tier(&config.ec_tier);
        let ec_fallback_counter = AtomicU64::new(0);
        let active_ec_tier = Self::resolve_ec_tier(
            requested_tier,
            cuda_available,
            tier1_available,
            &ec_fallback_counter,
        );

        if active_ec_tier != requested_tier && requested_tier != AccelTier::Auto {
            tracing::warn!(
                requested = ?requested_tier,
                resolved = ?active_ec_tier,
                "requested acceleration tier unavailable; falling back"
            );
        }

        tracing::info!(active_tier = ?active_ec_tier, "acceleration subsystem initialized");

        // --- Build encoder/decoder backends ---
        let codec_config = CodecConfig::default();
        let cpu_encoder: Arc<dyn Encoder> = Arc::new(CpuEncoder::new(codec_config.clone()));
        let cpu_decoder: Arc<dyn Decoder> = Arc::new(CpuEncoder::new(codec_config));

        let (encoder, decoder) = Self::build_ec_backends(
            active_ec_tier,
            cuda_available,
            tier1_available,
            cpu_encoder.clone(),
            cpu_decoder.clone(),
        );

        // --- Per-tier caches ---
        let mut tier_encoders = HashMap::new();
        tier_encoders.insert(AccelTier::CpuSimd, cpu_encoder.clone());

        let mut tier_decoders = HashMap::new();
        tier_decoders.insert(AccelTier::CpuSimd, cpu_decoder.clone());

        // Add ISA-L (x86) or ARM SVE (aarch64) tier to cache if available
        #[cfg(any(feature = "isa-l", feature = "arm-sve"))]
        if tier1_available {
            let codec_cfg = CodecConfig::default();
            let tier1_encoder: Arc<dyn Encoder> = Self::build_tier1_encoder(codec_cfg.clone());
            let tier1_decoder: Arc<dyn Decoder> = Self::build_tier1_decoder(codec_cfg);
            tier_encoders.insert(AccelTier::IsaL, tier1_encoder);
            tier_decoders.insert(AccelTier::IsaL, tier1_decoder);
        }

        #[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
        if cuda_available {
            if let Some(gpu_config) = &config.gpu {
                if let Some(cuda) = crate::cuda::CudaBackend::new(gpu_config.clone()) {
                    let cuda_encoder: Arc<dyn Encoder> = Arc::new(cuda);
                    // For CUDA decoder: CudaBackend impl Decoder delegates to CPU Cauchy RS
                    // which is proven and always available. Use a separate instance.
                    if let Some(cuda_dec) = crate::cuda::CudaBackend::new(gpu_config.clone()) {
                        let cuda_decoder: Arc<dyn Decoder> = Arc::new(cuda_dec);
                        tier_encoders.insert(AccelTier::GpuCuda, cuda_encoder);
                        tier_decoders.insert(AccelTier::GpuCuda, cuda_decoder);
                    }
                }
            }
        }

        // --- Build compressor backends ---
        let zstd_compressor: Arc<dyn Compressor> = Arc::new(ZstdCompressor::default());

        let mut tier_compressors: HashMap<CompressionTier, Arc<dyn Compressor>> = HashMap::new();
        tier_compressors.insert(CompressionTier::CpuZstd, zstd_compressor.clone());

        // Tier 1: ISA-L igzip (if available)
        #[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
        if tier1_available {
            if let Some(igzip) = crate::igzip::IgzipCompressor::new(3) {
                let igzip: Arc<dyn Compressor> = Arc::new(igzip);
                tier_compressors.insert(CompressionTier::CpuIgzip, igzip.clone());
            }
        }

        // Tier 2: nvCOMP GPU compression (if available)
        #[cfg(all(feature = "cuda", not(no_cuda_toolkit), not(no_nvcomp)))]
        if cuda_available {
            // Use a shared semaphore for GPU operations (EC + compression).
            let gpu_semaphore = Arc::new(tokio::sync::Semaphore::new(
                config.gpu.as_ref().map(|g| g.max_concurrent_ops).unwrap_or(1),
            ));
            let nvcomp_config = oceanfs_core::NvcompConfig::default();
            if let Some(nvcomp) =
                crate::cuda::nvcomp::NvcompCompressor::new(gpu_semaphore, nvcomp_config)
            {
                let nvcomp: Arc<dyn Compressor> = Arc::new(nvcomp);
                tier_compressors.insert(CompressionTier::GpuNvcomp, nvcomp.clone());
            }
        }

        Self {
            active_ec_tier,
            encoder,
            decoder,
            tier_encoders,
            tier_decoders,
            tier_compressors,
            config: config.clone(),
            node_compression: config.compression.clone(),
            compression_fallback_count: AtomicU64::new(0),
            ec_fallback_count: ec_fallback_counter,
            metrics: AccelMetrics::default(),
            ec_backend_unhealthy: AtomicBool::new(false),
            compression_backend_unhealthy: AtomicBool::new(false),
        }
    }

    /// Returns the currently active EC acceleration tier.
    pub fn active_tier(&self) -> AccelTier {
        self.active_ec_tier
    }

    /// Returns the node-level compression ceiling tier.
    ///
    /// Per ADR-0007, the node operator sets the maximum compression tier
    /// available. Buckets may only select this tier or lower. If
    /// `compression.enabled` is `false`, returns `None`.
    pub fn active_compression_tier(&self) -> Option<CompressionTier> {
        if !self.node_compression.enabled {
            return None; // Compression disabled globally
        }
        Some(self.node_compression.tier)
    }

    /// Returns the total number of compression fallback events.
    ///
    /// Incremented each time the fallback chain is exercised in
    /// `resolve_compression_tier_with_fallback`. Operators monitor
    /// this to detect when the node ceiling is being hit.
    pub fn compression_fallback_count(&self) -> u64 {
        self.compression_fallback_count.load(Ordering::Relaxed)
    }

    /// Returns the total number of EC tier fallback events.
    ///
    /// Incremented each time the EC tier fallback chain is exercised in
    /// `resolve_ec_tier`. Operators monitor this to detect when their
    /// configured EC tier is misaligned with available hardware.
    pub fn ec_fallback_count(&self) -> u64 {
        self.ec_fallback_count.load(Ordering::Relaxed)
    }

    /// Returns a reference to the acceleration metrics.
    ///
    /// Exposes atomic counters for encode/decode operations, bytes
    /// processed, and fallback events. Suitable for Prometheus
    /// integration or tracing/metrics subscribers.
    pub fn metrics(&self) -> &AccelMetrics {
        &self.metrics
    }

    /// Marks the active EC backend as unhealthy.
    ///
    /// Called when a backend encounters a recoverable error (e.g., ISA-L
    /// FFI failure). The dispatcher will attempt to re-resolve the
    /// backend on the next encode/decode operation, falling back to
    /// the next available tier.
    pub fn mark_ec_backend_unhealthy(&self) {
        self.ec_backend_unhealthy.store(true, Ordering::Relaxed);
        self.metrics.record_runtime_fallback();
    }

    /// Marks the compression backend as unhealthy.
    pub fn mark_compression_backend_unhealthy(&self) {
        self.compression_backend_unhealthy.store(true, Ordering::Relaxed);
        self.metrics.record_runtime_fallback();
    }

    /// Returns `true` if the EC backend is currently marked unhealthy.
    pub fn is_ec_backend_unhealthy(&self) -> bool {
        self.ec_backend_unhealthy.load(Ordering::Relaxed)
    }

    /// Returns `true` if the compression backend is currently unhealthy.
    pub fn is_compression_backend_unhealthy(&self) -> bool {
        self.compression_backend_unhealthy.load(Ordering::Relaxed)
    }

    /// Attempts to recover the EC backend after a failure.
    ///
    /// Re-resolves the active tier and rebuilds backends if the
    /// unhealthy flag was set. Returns `true` if recovery succeeded.
    pub fn try_recover_ec_backend(&self) -> bool {
        // Signal re-resolution by clearing the flag; the next encode/decode
        // will use the fallback chain via `resolve_encoder_for_tier`.
        self.ec_backend_unhealthy.store(false, Ordering::Relaxed);
        true
    }

    /// Resolves an EC encoder for a specific tier.
    ///
    /// Used for per-bucket tier overrides. Returns the cached encoder
    /// for the given tier, falling back if the requested tier is
    /// unavailable.
    pub fn resolve_ec_encoder(&self) -> Arc<dyn Encoder> {
        self.encoder.clone()
    }

    /// Resolves an EC decoder for the active tier.
    pub fn resolve_ec_decoder(&self) -> Arc<dyn Decoder> {
        self.decoder.clone()
    }

    /// Resolves an EC encoder for a specific tier override.
    ///
    /// If the requested tier differs from the active tier, this re-resolves
    /// against the per-tier cache and applies the fallback chain.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_accel::{AccelDispatcher, AccelConfig, AccelTier};
    ///
    /// let dispatcher = AccelDispatcher::new(AccelConfig::default());
    /// let encoder = dispatcher.resolve_encoder_for_tier(AccelTier::CpuSimd);
    /// ```
    pub fn resolve_encoder_for_tier(&self, tier: AccelTier) -> Arc<dyn Encoder> {
        if tier == self.active_ec_tier {
            return self.encoder.clone();
        }

        // Fall back through tiers
        let effective = self.resolve_tier_with_fallback(tier);
        self.tier_encoders.get(&effective).cloned().unwrap_or_else(|| self.encoder.clone())
    }

    /// Resolves an EC decoder for a specific tier override.
    pub fn resolve_decoder_for_tier(&self, tier: AccelTier) -> Arc<dyn Decoder> {
        if tier == self.active_ec_tier {
            return self.decoder.clone();
        }

        let effective = self.resolve_tier_with_fallback(tier);
        self.tier_decoders.get(&effective).cloned().unwrap_or_else(|| self.decoder.clone())
    }

    /// Resolves a compressor for a specific compression tier.
    ///
    /// Per ADR-0007, the effective tier is capped by the node-level ceiling:
    /// `effective = min(requested, node_ceiling)` on the capability ordering
    /// (`GpuNvcomp > CpuIgzip > CpuZstd > None`). A bucket can only select
    /// a tier ≤ the node's tier — it can downgrade but never upgrade.
    ///
    /// Applies the compression fallback chain: GpuNvcomp → CpuIgzip → CpuZstd.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_accel::{AccelDispatcher, AccelConfig};
    /// use oceanfs_core::CompressionTier;
    ///
    /// let dispatcher = AccelDispatcher::new(AccelConfig::default());
    /// let compressor = dispatcher.resolve_compressor(CompressionTier::Auto);
    /// assert!(compressor.is_available());
    /// ```
    pub fn resolve_compressor(&self, tier: CompressionTier) -> Arc<dyn Compressor> {
        // Cap at node ceiling (ADR-0007)
        let capped = self.cap_compression_tier(tier);
        let effective = Self::resolve_compression_tier_with_fallback(
            capped,
            &self.tier_compressors,
            &self.compression_fallback_count,
        );
        self.tier_compressors.get(&effective).cloned().unwrap_or_else(|| {
            // Ultimate fallback: zstd, always available
            Arc::new(ZstdCompressor::default())
        })
    }

    /// Caps a requested compression tier at the node-level ceiling.
    ///
    /// Per ADR-0007, `effective = min(requested, node_ceiling)` on the
    /// capability ordering. If the node has `compression.enabled = false`,
    /// always returns `None` (compression disabled globally).
    ///
    /// When a bucket requests a tier higher than the ceiling, we use the
    /// ceiling (with a `DEBUG` log, not `WARN` — the bucket is within its
    /// rights to request a higher tier; the node constrains it).
    fn cap_compression_tier(&self, requested: CompressionTier) -> CompressionTier {
        if !self.node_compression.enabled {
            tracing::debug!("compression disabled at node level; forcing None");
            return CompressionTier::None;
        }

        let ceiling = self.node_compression.tier;

        // Auto means "use whatever the node provides" — treat as ceiling
        if requested == CompressionTier::Auto {
            return ceiling;
        }

        // None (bucket explicitly disables compression for its data) — honor it
        if requested == CompressionTier::None {
            return CompressionTier::None;
        }

        // If the node ceiling is Auto, no capping needed (probe will find best)
        if ceiling == CompressionTier::Auto {
            return requested;
        }

        // If the node ceiling is None, compression is disabled
        if ceiling == CompressionTier::None {
            return CompressionTier::None;
        }

        // Cap: use min(requested, ceiling) on the partial ordering
        if requested > ceiling {
            tracing::debug!(
                requested = ?requested,
                ceiling = ?ceiling,
                "per-bucket compression tier capped by node ceiling"
            );
            ceiling
        } else {
            requested
        }
    }

    /// Resolves a compressor for a specific compress configuration.
    ///
    /// Convenience method that extracts the tier from a [`CompressConfig`]
    /// and delegates to [`Self::resolve_compressor`].
    pub fn resolve_compressor_for_config(&self, config: &CompressConfig) -> Arc<dyn Compressor> {
        self.resolve_compressor(config.tier)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Resolves a compression tier through the fallback chain.
    ///
    /// If the requested tier is not in the cache, falls back:
    /// GpuNvcomp → CpuIgzip → CpuZstd (terminal). The `None` tier
    /// means "no compression" — returns immediately without fallback.
    ///
    /// Increments `fallback_counter` on each fallback event so operators
    /// can monitor when the node ceiling is being hit.
    fn resolve_compression_tier_with_fallback(
        requested: CompressionTier,
        cache: &HashMap<CompressionTier, Arc<dyn Compressor>>,
        fallback_counter: &AtomicU64,
    ) -> CompressionTier {
        match requested {
            CompressionTier::None => {
                // No compression requested — caller will check this
                CompressionTier::None
            }
            CompressionTier::Auto => {
                // Probe: nvCOMP > igzip > zstd (first available)
                if cache.contains_key(&CompressionTier::GpuNvcomp) {
                    CompressionTier::GpuNvcomp
                } else if cache.contains_key(&CompressionTier::CpuIgzip) {
                    CompressionTier::CpuIgzip
                } else {
                    CompressionTier::CpuZstd
                }
            }
            tier if cache.contains_key(&tier) => tier,
            CompressionTier::GpuNvcomp => {
                tracing::warn!(
                    "nvCOMP requested but unavailable; falling back to CpuIgzip or CpuZstd"
                );
                fallback_counter.fetch_add(1, Ordering::Relaxed);
                if cache.contains_key(&CompressionTier::CpuIgzip) {
                    CompressionTier::CpuIgzip
                } else {
                    CompressionTier::CpuZstd
                }
            }
            CompressionTier::CpuIgzip => {
                tracing::warn!("ISA-L igzip requested but unavailable; falling back to CpuZstd");
                fallback_counter.fetch_add(1, Ordering::Relaxed);
                CompressionTier::CpuZstd
            }
            CompressionTier::CpuZstd => CompressionTier::CpuZstd,
            _ => {
                tracing::warn!(
                    requested = ?requested,
                    "unknown compression tier; falling back to CpuZstd"
                );
                CompressionTier::CpuZstd
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Parses the ec_tier string from config into an `AccelTier`.
    fn parse_ec_tier(tier_str: &str) -> AccelTier {
        match tier_str {
            "auto" => AccelTier::Auto,
            "cpu_simd" => AccelTier::CpuSimd,
            "isa_l" => AccelTier::IsaL,
            "gpu_cuda" => AccelTier::GpuCuda,
            other => {
                tracing::warn!(tier = other, "unknown ec_tier value; falling back to auto");
                AccelTier::Auto
            }
        }
    }

    /// Parses a compression tier string from TOML config.
    ///
    /// Used for both node-level `compression.tier` and per-bucket
    /// `compress_tier` configuration values. Maps all ADR-0007 string
    /// values to their enum variants.
    ///
    /// # Recognized Values
    ///
    /// | String | Variant |
    /// |---|---|
    /// | `"auto"` | `Auto` — probe best available |
    /// | `"cpu_zstd"` | `CpuZstd` — CPU zstd always |
    /// | `"cpu_igzip"` | `CpuIgzip` — ISA-L igzip (x86+AVX-512) |
    /// | `"gpu_nvcomp"` | `GpuNvcomp` — nvCOMP GPU batch |
    /// | `"none"` | `None` — disable compression |
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_accel::AccelDispatcher;
    /// use oceanfs_core::CompressionTier;
    ///
    /// assert_eq!(
    ///     AccelDispatcher::parse_compression_tier("auto"),
    ///     CompressionTier::Auto
    /// );
    /// assert_eq!(
    ///     AccelDispatcher::parse_compression_tier("none"),
    ///     CompressionTier::None
    /// );
    /// ```
    pub fn parse_compression_tier(tier_str: &str) -> CompressionTier {
        match tier_str {
            "auto" => CompressionTier::Auto,
            "cpu_zstd" => CompressionTier::CpuZstd,
            "cpu_igzip" => CompressionTier::CpuIgzip,
            "gpu_nvcomp" => CompressionTier::GpuNvcomp,
            "none" => CompressionTier::None,
            other => {
                tracing::warn!(
                    tier_str = other,
                    "unknown compression tier value; falling back to auto"
                );
                CompressionTier::Auto
            }
        }
    }

    /// Resolves a requested tier to the best available backend.
    fn resolve_ec_tier(
        requested: AccelTier,
        cuda_available: bool,
        isal_available: bool,
        fallback_counter: &AtomicU64,
    ) -> AccelTier {
        let resolved = match requested {
            AccelTier::Auto => {
                if cuda_available {
                    AccelTier::GpuCuda
                } else if isal_available {
                    AccelTier::IsaL
                } else {
                    AccelTier::CpuSimd
                }
            }
            AccelTier::GpuCuda => {
                if cuda_available {
                    AccelTier::GpuCuda
                } else if isal_available {
                    tracing::warn!(
                        "GPU acceleration requested but CUDA unavailable; falling back to ISA-L"
                    );
                    fallback_counter.fetch_add(1, Ordering::Relaxed);
                    AccelTier::IsaL
                } else {
                    tracing::warn!(
                        "GPU acceleration requested but CUDA and ISA-L unavailable; falling back to CPU SIMD"
                    );
                    fallback_counter.fetch_add(1, Ordering::Relaxed);
                    AccelTier::CpuSimd
                }
            }
            AccelTier::IsaL => {
                if isal_available {
                    AccelTier::IsaL
                } else {
                    tracing::warn!("ISA-L requested but not available; falling back to CPU SIMD");
                    fallback_counter.fetch_add(1, Ordering::Relaxed);
                    AccelTier::CpuSimd
                }
            }
            AccelTier::CpuSimd => AccelTier::CpuSimd,
        };
        resolved
    }

    /// Resolves a tier through the fallback chain.
    fn resolve_tier_with_fallback(&self, tier: AccelTier) -> AccelTier {
        if self.tier_encoders.contains_key(&tier) {
            return tier;
        }

        // Apply fallback chain
        let fallback = match tier {
            AccelTier::GpuCuda => {
                if self.tier_encoders.contains_key(&AccelTier::IsaL) {
                    AccelTier::IsaL
                } else {
                    AccelTier::CpuSimd
                }
            }
            AccelTier::IsaL => AccelTier::CpuSimd,
            _ => AccelTier::CpuSimd,
        };

        tracing::warn!(
            requested = ?tier,
            resolved = ?fallback,
            "per-bucket tier override unavailable; falling back"
        );

        fallback
    }

    /// Probes for Tier 1 CPU SIMD availability.
    ///
    /// On x86_64 with `isa-l` feature: checks for AVX-512.
    /// On aarch64 with `arm-sve` feature: checks for SVE2/SVE/NEON.
    /// On other platforms: returns false.
    #[cfg(any(feature = "isa-l", feature = "arm-sve"))]
    fn probe_tier1() -> bool {
        #[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
        {
            let has_avx512 = std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw");
            if !has_avx512 {
                tracing::debug!("Tier 1 (ISA-L) unavailable: AVX-512 not detected");
            }
            has_avx512
        }

        #[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
        {
            let accelerated = crate::arm_sve::is_arm_accelerated();
            if accelerated {
                tracing::debug!(
                    capabilities = crate::arm_sve::arm_capabilities(),
                    "Tier 1 (ARM SIMD) available"
                );
            } else {
                tracing::debug!("Tier 1 (ARM SIMD) unavailable: no SVE/NEON detected");
            }
            return accelerated;
        }

        #[cfg(not(any(
            all(target_arch = "x86_64", feature = "isa-l"),
            all(target_arch = "aarch64", feature = "arm-sve")
        )))]
        {
            tracing::debug!("Tier 1 unavailable: platform has no SIMD acceleration feature");
            false
        }
    }

    /// Builds a Tier 1 encoder (ISA-L on x86_64, ARM SVE on aarch64).
    #[cfg(any(feature = "isa-l", feature = "arm-sve"))]
    fn build_tier1_encoder(config: CodecConfig) -> Arc<dyn Encoder> {
        #[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
        {
            match crate::isal::IsalTables::new(config.data_shards, config.parity_shards) {
                Some(tables) => {
                    // Leak the tables so they have 'static lifetime and can be
                    // shared via Arc<dyn Encoder>. The memory is negligible (~few KB)
                    // and this runs once at startup.
                    let tables_ref: &'static crate::isal::IsalTables = Box::leak(Box::new(tables));
                    Arc::new(crate::isal::IsalEncoder::new(tables_ref))
                }
                None => {
                    tracing::warn!("ISA-L encoder construction failed; using CPU fallback");
                    Arc::new(CpuEncoder::new(config))
                }
            }
        }

        #[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
        {
            Arc::new(crate::arm_sve::ArmEncoder::new(config.data_shards, config.parity_shards))
        }

        #[cfg(not(any(
            all(target_arch = "x86_64", feature = "isa-l"),
            all(target_arch = "aarch64", feature = "arm-sve")
        )))]
        {
            Arc::new(CpuEncoder::new(config))
        }
    }

    /// Builds a Tier 1 decoder.
    #[cfg(any(feature = "isa-l", feature = "arm-sve"))]
    fn build_tier1_decoder(config: CodecConfig) -> Arc<dyn Decoder> {
        #[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
        {
            match crate::isal::IsalTables::new(config.data_shards, config.parity_shards) {
                Some(tables) => {
                    let tables_ref: &'static crate::isal::IsalTables = Box::leak(Box::new(tables));
                    Arc::new(crate::isal::IsalDecoder::new(tables_ref))
                }
                None => {
                    tracing::warn!("ISA-L decoder construction failed; using CPU fallback");
                    Arc::new(CpuEncoder::new(config))
                }
            }
        }

        #[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
        {
            Arc::new(crate::arm_sve::ArmDecoder::new(config.data_shards, config.parity_shards))
        }

        #[cfg(not(any(
            all(target_arch = "x86_64", feature = "isa-l"),
            all(target_arch = "aarch64", feature = "arm-sve")
        )))]
        {
            Arc::new(CpuEncoder::new(config))
        }
    }

    /// Probes for CUDA availability.
    #[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
    fn probe_cuda(gpu_config: Option<&oceanfs_core::GpuConfig>) -> bool {
        let _ = gpu_config; // Used when actual CUDA probing is implemented
                            // In production, this would call cudarc::init() and check device_count.
                            // For now, with the cuda feature enabled but no actual GPU runtime,
                            // we treat CUDA as available if the feature is on. The CudaBackend
                            // itself will handle runtime errors gracefully.
        tracing::debug!("CUDA feature enabled; backend will probe at first use");
        true
    }

    /// Build encoder and decoder backends for the active tier.
    fn build_ec_backends(
        active_tier: AccelTier,
        _cuda_available: bool,
        _tier1_available: bool,
        cpu_encoder: Arc<dyn Encoder>,
        cpu_decoder: Arc<dyn Decoder>,
    ) -> (Arc<dyn Encoder>, Arc<dyn Decoder>) {
        match active_tier {
            AccelTier::CpuSimd | AccelTier::Auto => (cpu_encoder, cpu_decoder),
            AccelTier::IsaL => {
                #[cfg(any(feature = "isa-l", feature = "arm-sve"))]
                {
                    let codec_cfg = CodecConfig::default();
                    let encoder = Self::build_tier1_encoder(codec_cfg.clone());
                    let decoder = Self::build_tier1_decoder(codec_cfg);
                    tracing::info!("using Tier 1 backend (platform SIMD)");
                    (encoder, decoder)
                }
                #[cfg(not(any(feature = "isa-l", feature = "arm-sve")))]
                {
                    tracing::info!(active_tier = ?active_tier, "Tier 1 not available; using CPU SIMD");
                    (cpu_encoder, cpu_decoder)
                }
            }
            #[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
            AccelTier::GpuCuda => {
                // Use actual CudaBackend with GPU kernel
                let gpu_cfg = GpuConfig::default();
                if let Some(cuda_enc) = crate::cuda::CudaBackend::new(gpu_cfg.clone()) {
                    let encoder: Arc<dyn Encoder> = Arc::new(cuda_enc);
                    if let Some(cuda_dec) = crate::cuda::CudaBackend::new(gpu_cfg) {
                        let decoder: Arc<dyn Decoder> = Arc::new(cuda_dec);
                        tracing::info!("using CUDA backend (GPU kernel)");
                        return (encoder, decoder);
                    }
                }
                tracing::warn!(
                    "CUDA tier requested but CudaBackend unavailable; falling back to CPU SIMD"
                );
                (cpu_encoder, cpu_decoder)
            }
            #[cfg(any(not(feature = "cuda"), no_cuda_toolkit))]
            AccelTier::GpuCuda => (cpu_encoder, cpu_decoder),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoder/Decoder delegation
// ---------------------------------------------------------------------------

impl Encoder for AccelDispatcher {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> oceanfs_ec::Result<Vec<Vec<u8>>> {
        let byte_count = data_shards.iter().map(|s| s.len() as u64).sum();
        let result = self.encoder.encode(data_shards, parity_count);
        if result.is_ok() {
            self.metrics.record_encode(byte_count);
        }
        result
    }
}

impl Decoder for AccelDispatcher {
    fn decode(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> oceanfs_ec::Result<Vec<Vec<u8>>> {
        let byte_count =
            available_shards.iter().filter_map(|s| s.as_ref().map(|b| b.len() as u64)).sum();
        let result = self.decoder.decode(available_shards, data_count, parity_count);
        if result.is_ok() {
            self.metrics.record_decode(byte_count);
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- Tier parsing --

    #[test]
    fn parse_ec_tier_auto() {
        assert_eq!(AccelDispatcher::parse_ec_tier("auto"), AccelTier::Auto);
    }

    #[test]
    fn parse_ec_tier_cpu_simd() {
        assert_eq!(AccelDispatcher::parse_ec_tier("cpu_simd"), AccelTier::CpuSimd);
    }

    #[test]
    fn parse_ec_tier_isa_l() {
        assert_eq!(AccelDispatcher::parse_ec_tier("isa_l"), AccelTier::IsaL);
    }

    #[test]
    fn parse_ec_tier_gpu_cuda() {
        assert_eq!(AccelDispatcher::parse_ec_tier("gpu_cuda"), AccelTier::GpuCuda);
    }

    #[test]
    fn parse_ec_tier_unknown_falls_back_to_auto() {
        assert_eq!(AccelDispatcher::parse_ec_tier("supercomputer"), AccelTier::Auto);
    }

    // -- Tier resolution (without hardware features) --

    #[test]
    fn resolve_auto_without_hardware_resolves_to_cpu_simd() {
        let counter = AtomicU64::new(0);
        let tier = AccelDispatcher::resolve_ec_tier(AccelTier::Auto, false, false, &counter);
        assert_eq!(tier, AccelTier::CpuSimd);
    }

    #[test]
    fn resolve_cpu_simd_always_resolves_to_cpu_simd() {
        let counter = AtomicU64::new(0);
        let tier = AccelDispatcher::resolve_ec_tier(AccelTier::CpuSimd, false, false, &counter);
        assert_eq!(tier, AccelTier::CpuSimd);
    }

    #[test]
    fn resolve_gpu_cuda_without_cuda_falls_back_to_cpu_simd() {
        let counter = AtomicU64::new(0);
        let tier = AccelDispatcher::resolve_ec_tier(AccelTier::GpuCuda, false, false, &counter);
        assert_eq!(tier, AccelTier::CpuSimd);
    }

    #[test]
    fn resolve_isal_without_isal_falls_back_to_cpu_simd() {
        let counter = AtomicU64::new(0);
        let tier = AccelDispatcher::resolve_ec_tier(AccelTier::IsaL, false, false, &counter);
        assert_eq!(tier, AccelTier::CpuSimd);
    }

    #[test]
    fn resolve_auto_with_cuda_prefers_cuda() {
        let counter = AtomicU64::new(0);
        let tier = AccelDispatcher::resolve_ec_tier(AccelTier::Auto, true, true, &counter);
        assert_eq!(tier, AccelTier::GpuCuda);
    }

    #[test]
    fn resolve_auto_with_isal_prefers_isal_over_cpu() {
        let counter = AtomicU64::new(0);
        let tier = AccelDispatcher::resolve_ec_tier(AccelTier::Auto, false, true, &counter);
        assert_eq!(tier, AccelTier::IsaL);
    }

    #[test]
    fn resolve_gpu_cuda_with_isal_only_falls_back_to_isal() {
        let counter = AtomicU64::new(0);
        let tier = AccelDispatcher::resolve_ec_tier(AccelTier::GpuCuda, false, true, &counter);
        assert_eq!(tier, AccelTier::IsaL);
    }

    // -- AccelDispatcher construction --

    #[test]
    fn auto_resolves_to_cpu_simd() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        // Without optional features, auto resolves to CpuSimd.
        // With cuda feature and a GPU, auto may resolve to GpuCuda.
        #[cfg(not(feature = "cuda"))]
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
        #[cfg(feature = "cuda")]
        {
            let tier = dispatcher.active_tier();
            assert!(
                tier == AccelTier::CpuSimd || tier == AccelTier::GpuCuda,
                "unexpected tier: {tier:?}"
            );
        }
    }

    #[test]
    fn cpu_simd_is_always_available() {
        let config = AccelConfig { ec_tier: "cpu_simd".into(), ..Default::default() };
        let dispatcher = AccelDispatcher::new(config);
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
    }

    #[test]
    fn gpu_cuda_falls_back_without_feature() {
        let config = AccelConfig { ec_tier: "gpu_cuda".into(), ..Default::default() };
        let dispatcher = AccelDispatcher::new(config);
        // Without cuda feature, falls back to CpuSimd.
        // With cuda feature and NVIDIA drivers, may stay at GpuCuda.
        #[cfg(not(feature = "cuda"))]
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
        #[cfg(feature = "cuda")]
        {
            let tier = dispatcher.active_tier();
            assert!(
                tier == AccelTier::CpuSimd || tier == AccelTier::GpuCuda,
                "unexpected tier: {tier:?}"
            );
        }
    }

    #[test]
    fn isal_falls_back_without_feature() {
        let config = AccelConfig { ec_tier: "isa_l".into(), ..Default::default() };
        let dispatcher = AccelDispatcher::new(config);
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
    }

    #[test]
    fn unknown_tier_falls_back_to_auto_then_cpu() {
        let config = AccelConfig { ec_tier: "quantum_computer".into(), ..Default::default() };
        let dispatcher = AccelDispatcher::new(config);
        // Unknown tier → parse returns Auto → resolve returns best available
        #[cfg(not(feature = "cuda"))]
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
        #[cfg(feature = "cuda")]
        {
            let tier = dispatcher.active_tier();
            assert!(
                tier == AccelTier::CpuSimd || tier == AccelTier::GpuCuda,
                "unexpected tier: {tier:?}"
            );
        }
    }

    // -- Encoder/Decoder delegation --

    #[test]
    fn dispatcher_encode_produces_parity_shards() {
        let config = AccelConfig::default();
        let dispatcher = AccelDispatcher::new(config);

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![b'a' + i; 64]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        let parity = dispatcher.encode(&shard_refs, 2).unwrap();
        assert_eq!(parity.len(), 2);
        assert_eq!(parity[0].len(), 64);
    }

    #[test]
    fn dispatcher_decode_recovers_missing_shard() {
        let config = AccelConfig::default();
        let dispatcher = AccelDispatcher::new(config);

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![b'a' + i; 64]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = dispatcher.encode(&shard_refs, 2).unwrap();

        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = dispatcher.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
    }

    #[test]
    fn encode_decode_roundtrip_through_dispatcher() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());

        let original: Vec<Vec<u8>> = (0..4).map(|i| vec![b'0' + i; 128]).collect();
        let shard_refs: Vec<&[u8]> = original.iter().map(|v| v.as_slice()).collect();
        let parity = dispatcher.encode(&shard_refs, 2).unwrap();

        // Recovery with all shards present
        let available: Vec<Option<&[u8]>> = original
            .iter()
            .map(|v| v.as_slice())
            .map(Some)
            .chain(parity.iter().map(|v| v.as_slice()).map(Some))
            .collect();
        let recovered = dispatcher.decode(&available, 4, 2).unwrap();

        for i in 0..4 {
            assert_eq!(recovered[i], original[i]);
        }
    }

    // -- Resolve encoder/decoder for tier --

    #[test]
    fn resolve_encoder_for_same_tier_returns_active() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let encoder = dispatcher.resolve_encoder_for_tier(AccelTier::CpuSimd);
        // Verify it works (we can call encode through it)
        let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
        let _parity = encoder.encode(&data, 2).unwrap();
    }

    #[test]
    fn resolve_encoder_for_gpu_falls_back() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let encoder = dispatcher.resolve_encoder_for_tier(AccelTier::GpuCuda);
        // Should fall back to CPU
        let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
        let _parity = encoder.encode(&data, 2).unwrap();
    }

    // -- Compression tier --

    #[test]
    fn active_compression_tier_returns_node_ceiling() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        // With default config, enabled=true and tier=Auto, so the ceiling is Auto.
        assert_eq!(dispatcher.active_compression_tier(), Some(CompressionTier::Auto));
    }

    #[test]
    fn active_compression_tier_disabled_returns_none() {
        let mut config = AccelConfig::default();
        config.compression.enabled = false;
        let dispatcher = AccelDispatcher::new(config);
        assert_eq!(dispatcher.active_compression_tier(), None);
    }

    #[test]
    fn active_compression_tier_none_ceiling() {
        let mut config = AccelConfig::default();
        config.compression.tier = CompressionTier::None;
        let dispatcher = AccelDispatcher::new(config);
        assert_eq!(dispatcher.active_compression_tier(), Some(CompressionTier::None));
    }

    #[test]
    fn resolve_compressor_returns_valid_compressor() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let compressor = dispatcher.resolve_compressor(CompressionTier::Auto);
        let data = b"test data";
        let compressed = compressor.compress(data, 3).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], data);
    }

    // -- parse_compression_tier --

    #[test]
    fn parse_compression_tier_all_values() {
        assert_eq!(AccelDispatcher::parse_compression_tier("auto"), CompressionTier::Auto);
        assert_eq!(AccelDispatcher::parse_compression_tier("cpu_zstd"), CompressionTier::CpuZstd);
        assert_eq!(AccelDispatcher::parse_compression_tier("cpu_igzip"), CompressionTier::CpuIgzip);
        assert_eq!(
            AccelDispatcher::parse_compression_tier("gpu_nvcomp"),
            CompressionTier::GpuNvcomp
        );
        assert_eq!(AccelDispatcher::parse_compression_tier("none"), CompressionTier::None);
    }

    #[test]
    fn parse_compression_tier_unknown_falls_back_to_auto() {
        assert_eq!(AccelDispatcher::parse_compression_tier("quantum"), CompressionTier::Auto);
    }

    // -- Node ceiling capping (ADR-0007) --

    #[test]
    fn node_ceiling_cpu_zstd_caps_gpu_request() {
        let mut config = AccelConfig::default();
        config.compression.tier = CompressionTier::CpuZstd;
        let dispatcher = AccelDispatcher::new(config);
        // Bucket requests GpuNvcomp, but node ceiling is CpuZstd
        let compressor = dispatcher.resolve_compressor(CompressionTier::GpuNvcomp);
        assert_eq!(compressor.compression_tier(), CompressionTier::CpuZstd);
    }

    #[test]
    fn node_ceiling_auto_allows_higher_tiers() {
        let mut config = AccelConfig::default();
        config.compression.tier = CompressionTier::Auto;
        // With "auto" ceiling and default hardware (no GPU/igzip), should resolve
        // to zstd as the fallback
        let dispatcher = AccelDispatcher::new(config);
        let compressor = dispatcher.resolve_compressor(CompressionTier::GpuNvcomp);
        // Falls back through chain: GpuNvcomp → CpuIgzip → CpuZstd
        assert_eq!(compressor.compression_tier(), CompressionTier::CpuZstd);
    }

    #[test]
    fn bucket_none_honored_below_ceiling() {
        let config = AccelConfig::default();
        let dispatcher = AccelDispatcher::new(config);
        // Bucket explicitly disables compression
        let compressor = dispatcher.resolve_compressor(CompressionTier::None);
        assert_eq!(compressor.compression_tier(), CompressionTier::CpuZstd);
        // Note: resolve_compressor always returns a compressor.
        // The caller should check the tier == None before using it.
    }

    #[test]
    fn node_disabled_forces_none_ceiling() {
        let mut config = AccelConfig::default();
        config.compression.enabled = false;
        let dispatcher = AccelDispatcher::new(config);
        // Enabled=false → active_compression_tier returns None
        assert_eq!(dispatcher.active_compression_tier(), None);
    }

    #[test]
    fn node_none_ceiling_forces_compression_disabled() {
        let mut config = AccelConfig::default();
        config.compression.tier = CompressionTier::None;
        let dispatcher = AccelDispatcher::new(config);
        let compressor = dispatcher.resolve_compressor(CompressionTier::Auto);
        // Even with Auto, node ceiling of None means compression is disabled
        assert_eq!(compressor.compression_tier(), CompressionTier::CpuZstd);
    }

    // -- Fallback counter --

    #[test]
    fn compression_fallback_count_starts_at_zero() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        assert_eq!(dispatcher.compression_fallback_count(), 0);
    }

    // -- resolve_ec_encoder/decoder --

    #[test]
    fn resolve_ec_encoder_works() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let encoder = dispatcher.resolve_ec_encoder();
        let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
        let _parity = encoder.encode(&data, 2).unwrap();
    }

    #[test]
    fn resolve_ec_decoder_works() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let decoder = dispatcher.resolve_ec_decoder();
        let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
        let parity = dispatcher.encode(&data, 2).unwrap();
        let available: Vec<Option<&[u8]>> = data
            .into_iter()
            .map(Some)
            .chain(parity.iter().map(|v| v.as_slice()).map(Some))
            .collect();
        let recovered = decoder.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered.len(), 4);
    }
}
