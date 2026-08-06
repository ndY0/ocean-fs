//! Tiered segment routing — dispatches blob writes to the correct tier.

use oceanfs_core::{SegmentSizeConfig, SizeTier};

/// Routes blob writes to the appropriate storage tier.
pub struct TierRouter {
    config: SegmentSizeConfig,
}

impl TierRouter {
    /// Creates a new tier router from the given configuration.
    pub fn new(config: SegmentSizeConfig) -> Self {
        Self { config }
    }

    /// Classifies a blob of the given size into a storage tier.
    pub fn classify(&self, blob_size: u64) -> SizeTier {
        self.config.classify(blob_size)
    }

    /// Returns `true` if the blob should be stored inline.
    pub fn is_inline(&self, blob_size: u64) -> bool {
        matches!(self.classify(blob_size), SizeTier::Inline)
    }

    /// Returns the target segment size for the given tier.
    pub fn target_size(&self, tier: SizeTier) -> u64 {
        match tier {
            SizeTier::Small => self.config.small_target_size,
            SizeTier::Standard | SizeTier::Multi => self.config.default_target_size,
            SizeTier::Inline => 0,
            _ => self.config.default_target_size,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn router_classify_uses_config_thresholds() {
        let config = SegmentSizeConfig::default();
        let router = TierRouter::new(config);
        assert_eq!(router.classify(1024), SizeTier::Inline);
        assert_eq!(router.classify(4097), SizeTier::Small);
        assert_eq!(router.classify(500_000), SizeTier::Standard);
        assert_eq!(router.classify(10_000_000), SizeTier::Multi);
    }

    #[test]
    fn is_inline_true_for_small_blobs() {
        let router = TierRouter::new(SegmentSizeConfig::default());
        assert!(router.is_inline(100));
        assert!(!router.is_inline(5000));
    }
}
