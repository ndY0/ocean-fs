//! Acceleration subsystem configuration.
//!
//! Controls EC encoding tier, hash tier, GPU-specific options,
//! and node-level compression governance.

use crate::{config::CompressionConfig, types::GpuConfig};

/// Configuration for the acceleration subsystem.
///
/// Controls EC encoding tier, hash tier, GPU-specific options,
/// and node-level compression governance. Loaded from the
/// `[acceleration]` and `[compression]` sections of `oceanfs.toml`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{AccelConfig, GpuConfig};
///
/// let config = AccelConfig::default();
/// assert!(config.ec_tier_is_auto());
/// ```
#[derive(Debug, Clone)]
pub struct AccelConfig {
    /// EC acceleration tier: "auto", "cpu_simd", "isa_l", or "gpu_cuda".
    pub ec_tier: String,
    /// Hash acceleration tier: "auto" or "avx512" (delegates to blake3 crate).
    pub hash_tier: String,
    /// GPU-specific configuration (None if no GPU config is provided).
    pub gpu: Option<GpuConfig>,
    /// Prefer AVX-512 code path in ISA-L if available (default true).
    pub isal_prefer_avx512: bool,
    /// Node-level compression governance (per ADR-0007).
    /// Controls the compression ceiling — buckets may only select
    /// this tier or lower. Default: enabled, tier=auto.
    pub compression: CompressionConfig,
}

impl Default for AccelConfig {
    fn default() -> Self {
        Self {
            ec_tier: "auto".into(),
            hash_tier: "auto".into(),
            gpu: None,
            isal_prefer_avx512: true,
            compression: CompressionConfig::default(),
        }
    }
}

impl AccelConfig {
    /// Returns `true` if the EC tier is set to `"auto"`.
    pub fn ec_tier_is_auto(&self) -> bool {
        self.ec_tier == "auto"
    }

    /// Returns `true` if GPU configuration is provided.
    pub fn has_gpu_config(&self) -> bool {
        self.gpu.is_some()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn accel_config_default_is_auto() {
        let cfg = AccelConfig::default();
        assert!(cfg.ec_tier_is_auto());
        assert_eq!(cfg.hash_tier, "auto");
        assert!(cfg.gpu.is_none());
        assert!(cfg.isal_prefer_avx512);
    }

    #[test]
    fn accel_config_has_gpu_config() {
        let cfg = AccelConfig {
            ec_tier: "gpu_cuda".into(),
            hash_tier: "auto".into(),
            gpu: Some(GpuConfig::default()),
            isal_prefer_avx512: true,
            compression: CompressionConfig::default(),
        };
        assert!(cfg.has_gpu_config());
    }

    #[test]
    fn accel_config_not_auto() {
        let cfg = AccelConfig {
            ec_tier: "cpu_simd".into(),
            hash_tier: "auto".into(),
            gpu: None,
            isal_prefer_avx512: false,
            compression: CompressionConfig::default(),
        };
        assert!(!cfg.ec_tier_is_auto());
        assert!(!cfg.has_gpu_config());
    }
}
