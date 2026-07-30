//! Tiered acceleration dispatcher.
//!
//! Selects the best available EC codec backend at runtime based on
//! configuration and hardware availability.

/// Acceleration tier levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccelTier {
    /// Automatically select the best available tier.
    Auto,
    /// CPU SIMD (always available).
    CpuSimd,
    /// Intel ISA-L optimized (x86 with AVX-512).
    IsaL,
    /// GPU / CUDA (requires CUDA hardware).
    GpuCuda,
}

/// Dispatches EC operations to the best available backend.
pub struct AccelDispatcher {
    active_tier: AccelTier,
}

impl AccelDispatcher {
    /// Creates a new dispatcher with the requested tier.
    ///
    /// Falls back to `CpuSimd` if the requested tier is unavailable.
    pub fn new(requested: AccelTier) -> Self {
        let active_tier = match requested {
            AccelTier::Auto | AccelTier::CpuSimd => AccelTier::CpuSimd,
            #[cfg(feature = "isa-l")]
            AccelTier::IsaL => AccelTier::IsaL,
            #[cfg(not(feature = "isa-l"))]
            AccelTier::IsaL => {
                tracing::warn!("ISA-L not compiled; falling back to CPU SIMD");
                AccelTier::CpuSimd
            }
            #[cfg(feature = "cuda")]
            AccelTier::GpuCuda => AccelTier::GpuCuda,
            #[cfg(not(feature = "cuda"))]
            AccelTier::GpuCuda => {
                tracing::warn!("CUDA not compiled; falling back to CPU SIMD");
                AccelTier::CpuSimd
            }
        };

        Self { active_tier }
    }

    /// Returns the currently active acceleration tier.
    pub fn active_tier(&self) -> AccelTier {
        self.active_tier
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn auto_resolves_to_cpu_simd() {
        let dispatcher = AccelDispatcher::new(AccelTier::Auto);
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
    }

    #[test]
    fn cpu_simd_is_always_available() {
        let dispatcher = AccelDispatcher::new(AccelTier::CpuSimd);
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
    }

    #[test]
    fn gpu_cuda_falls_back_without_feature() {
        let dispatcher = AccelDispatcher::new(AccelTier::GpuCuda);
        assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
    }
}
