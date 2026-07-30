//! Tiered segment routing — dispatches blob writes to the correct tier.
//!
//! Routes blobs to inline storage (metadata), small segments, standard
//! segments, or multi-segment splitting based on blob size.

use oceanfs_core::{ChunkRef, SegmentSizeConfig, SizeTier};

/// Routes blob writes to the appropriate storage tier.
///
/// Wraps [`SegmentSizeConfig::classify`] to provide the routing decision
/// used by the write path.
pub(crate) struct TierRouter {
    config: SegmentSizeConfig,
}

#[allow(dead_code)]
impl TierRouter {
    /// Creates a new tier router.
    pub(crate) fn new(config: SegmentSizeConfig) -> Self {
        Self { config }
    }

    /// Classifies a blob size into its appropriate storage tier.
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `blob_size` is zero.
    pub(crate) fn classify(&self, blob_size: u64) -> SizeTier {
        self.config.classify(blob_size)
    }

    /// Returns `true` if the blob should be stored inline.
    pub(crate) fn is_inline(&self, blob_size: u64) -> bool {
        matches!(self.classify(blob_size), SizeTier::Inline)
    }

    /// Returns the target segment size for a given tier.
    pub(crate) fn target_size(&self, tier: SizeTier) -> u64 {
        match tier {
            SizeTier::Small => self.config.small_target_size,
            SizeTier::Standard | SizeTier::Multi => self.config.default_target_size,
            SizeTier::Inline => 0,
            _ => self.config.default_target_size,
        }
    }
}

/// Builds a list of `ChunkRef`s from segment append operations.
#[allow(dead_code)]
pub(crate) struct ChunkListBuilder;

#[allow(dead_code)]
impl ChunkListBuilder {
    /// Creates chunk refs from a single-segment write.
    pub(crate) fn single(
        segment_id: oceanfs_core::SegmentId,
        offset: u64,
        length: u32,
    ) -> smallvec::SmallVec<[ChunkRef; 4]> {
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id, offset, length });
        chunks
    }

    /// Creates chunk refs from a multi-segment write.
    pub(crate) fn multi(
        refs: Vec<(oceanfs_core::SegmentId, u64, u32)>,
    ) -> smallvec::SmallVec<[ChunkRef; 4]> {
        let mut chunks = smallvec::SmallVec::with_capacity(refs.len());
        for (segment_id, offset, length) in refs {
            chunks.push(ChunkRef { segment_id, offset, length });
        }
        chunks
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

    #[test]
    fn chunk_list_builder_single() {
        let id = oceanfs_core::SegmentId::new();
        let chunks = ChunkListBuilder::single(id, 0, 1024);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].segment_id, id);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].length, 1024);
    }

    #[test]
    fn chunk_list_builder_multi() {
        let id1 = oceanfs_core::SegmentId::new();
        let id2 = oceanfs_core::SegmentId::new();
        let chunks = ChunkListBuilder::multi(vec![(id1, 0, 100), (id2, 0, 50)]);
        assert_eq!(chunks.len(), 2);
    }
}
