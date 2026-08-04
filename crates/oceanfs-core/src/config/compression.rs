//! Node-level compression governance configuration.
//!
//! Per ADR-0007, the node operator controls what compression backends
//! are available. The node-level `tier` sets the **ceiling**.

use crate::types::CompressionTier;

/// Node-level compression governance configuration.
///
/// Per ADR-0007, the node operator controls what compression backends
/// are available. The node-level `tier` sets the **ceiling** — the
/// maximum tier any bucket may use. Per-bucket `compress_tier` can only
/// select from or downgrade from the node ceiling; it cannot upgrade.
///
/// Loaded from the `[compression]` section of `oceanfs.toml`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{CompressionConfig, CompressionTier};
///
/// let config = CompressionConfig::default();
/// assert!(config.enabled);
/// assert_eq!(config.tier, CompressionTier::Auto);
/// ```
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Whether segment compression is enabled at all.
    /// When `false`, no compression is applied regardless of bucket settings.
    pub enabled: bool,
    /// Compression acceleration tier ceiling for this node.
    /// Buckets may only select this tier or lower.
    pub tier: CompressionTier,
    /// Minimum batch bytes for GPU offload (only relevant when tier ≥ GpuNvcomp).
    pub gpu_min_batch_bytes: u64,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self { enabled: true, tier: CompressionTier::Auto, gpu_min_batch_bytes: 1_048_576 }
    }
}
