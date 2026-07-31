//! Integration test: Tiered segment routing & multi-segment splitting.
//!
//! Verifies:
//! - TierRouter::classify() at boundary sizes
//! - SizeTier threshold table implementation
//! - SegmentSplitter chunk boundaries
//! - ChunkListBuilder (indirectly through classify + splitter)
//!
//! Covers the `tiered-segment-routing` feature's Definition of Done.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use oceanfs_core::{SegmentSizeConfig, SizeTier};
use oceanfs_storage::{SegmentSplitter, TierRouter};

fn default_config() -> SegmentSizeConfig {
    SegmentSizeConfig::default()
}

// Helper to classify a blob using default config.
fn classify(size: u64) -> SizeTier {
    let router = TierRouter::new(default_config());
    router.classify(size)
}

// ---------------------------------------------------------------------------
// TierRouter::classify — boundary tests
// ---------------------------------------------------------------------------

#[test]
fn classify_zero_bytes_is_error() {
    // 0-byte blobs are rejected by the size config (asserts blob_size > 0).
    // The write path handles empty data before classification.
    let config = SegmentSizeConfig::default();
    let result = std::panic::catch_unwind(|| {
        let _ = config.classify(0);
    });
    assert!(result.is_err(), "0-byte size should be rejected");
}

#[test]
fn classify_at_inline_threshold() {
    // Default: inline_threshold_bytes = 4096
    assert_eq!(classify(4096), SizeTier::Inline);
}

#[test]
fn classify_just_above_inline_threshold() {
    // 4097 > 4096 → Small tier.
    assert_eq!(classify(4097), SizeTier::Small);
}

#[test]
fn classify_at_small_threshold() {
    // Default: segment_small_threshold_bytes = 262144 (256 KB)
    assert_eq!(classify(262144), SizeTier::Small);
}

#[test]
fn classify_just_above_small_threshold() {
    // 262145 > 256 KB → Standard tier.
    assert_eq!(classify(262145), SizeTier::Standard);
}

#[test]
fn classify_at_standard_threshold() {
    // Default: default_target_size = 4194304 (4 MB)
    assert_eq!(classify(4_194_304), SizeTier::Standard);
}

#[test]
fn classify_just_above_standard_threshold() {
    // > 4 MB → Multi tier.
    assert_eq!(classify(4_194_305), SizeTier::Multi);
}

#[test]
fn classify_typical_1kb_blob() {
    assert_eq!(classify(1024), SizeTier::Inline);
}

#[test]
fn classify_typical_64kb_blob() {
    // 64 KB is between inline (4 KB) and small (256 KB).
    assert_eq!(classify(65536), SizeTier::Small);
}

#[test]
fn classify_typical_1mb_blob() {
    // 1 MB is between small (256 KB) and standard (4 MB).
    assert_eq!(classify(1_048_576), SizeTier::Standard);
}

#[test]
fn classify_typical_10mb_blob() {
    // 10 MB > 4 MB → Multi.
    assert_eq!(classify(10_485_760), SizeTier::Multi);
}

// ---------------------------------------------------------------------------
// TierRouter::is_inline
// ---------------------------------------------------------------------------

#[test]
fn is_inline_true_for_small_blobs() {
    let router = TierRouter::new(default_config());
    assert!(router.is_inline(1));
    assert!(router.is_inline(4096));
    assert!(!router.is_inline(4097));
    assert!(!router.is_inline(1_048_576));
}

// ---------------------------------------------------------------------------
// TierRouter with custom config
// ---------------------------------------------------------------------------

#[test]
fn custom_thresholds_are_respected() {
    let config = SegmentSizeConfig {
        inline_threshold_bytes: 1024,
        small_threshold_bytes: 10_000,
        small_target_size: 5_000,
        default_target_size: 100_000,
    };

    let router = TierRouter::new(config);

    assert_eq!(router.classify(512), SizeTier::Inline);
    assert_eq!(router.classify(1024), SizeTier::Inline);
    assert_eq!(router.classify(1025), SizeTier::Small);
    assert_eq!(router.classify(10_000), SizeTier::Small);
    assert_eq!(router.classify(10_001), SizeTier::Standard);
    assert_eq!(router.classify(100_000), SizeTier::Standard);
    assert_eq!(router.classify(100_001), SizeTier::Multi);
}

// ---------------------------------------------------------------------------
// TierRouter::target_size
// ---------------------------------------------------------------------------

#[test]
fn target_size_per_tier() {
    let config = SegmentSizeConfig {
        small_target_size: 65536,
        default_target_size: 4_194_304,
        ..SegmentSizeConfig::default()
    };
    let router = TierRouter::new(config);

    assert_eq!(router.target_size(SizeTier::Inline), 0);
    assert_eq!(router.target_size(SizeTier::Small), 65536);
    assert_eq!(router.target_size(SizeTier::Standard), 4_194_304);
    assert_eq!(router.target_size(SizeTier::Multi), 4_194_304);
}

// ---------------------------------------------------------------------------
// SegmentSplitter tests
// ---------------------------------------------------------------------------

#[test]
fn split_exact_chunks() {
    let splitter = SegmentSplitter::new(10);
    let data = [1u8; 30];
    let chunks = splitter.split(&data);

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].0, 0);
    assert_eq!(chunks[0].1.len(), 10);
    assert_eq!(chunks[1].0, 10);
    assert_eq!(chunks[1].1.len(), 10);
    assert_eq!(chunks[2].0, 20);
    assert_eq!(chunks[2].1.len(), 10);
}

#[test]
fn split_uneven_last_chunk() {
    let splitter = SegmentSplitter::new(10);
    let data = [1u8; 25];
    let chunks = splitter.split(&data);

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].1.len(), 10);
    assert_eq!(chunks[1].1.len(), 10);
    assert_eq!(chunks[2].1.len(), 5);
}

#[test]
fn split_smaller_than_chunk() {
    let splitter = SegmentSplitter::new(1_000_000);
    let data = [1u8; 5];
    let chunks = splitter.split(&data);

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].0, 0);
    assert_eq!(chunks[0].1.len(), 5);
}

#[test]
fn split_empty_returns_empty() {
    let splitter = SegmentSplitter::new(100);
    let chunks = splitter.split(&[]);
    assert!(chunks.is_empty());
}

#[test]
fn split_large_blob_boundary() {
    // 4 MB default target → one blob = one chunk
    let splitter = SegmentSplitter::new(4_194_304);
    let data = vec![0u8; 4_194_304]; // exactly 4 MB
    let chunks = splitter.split(&data);
    assert_eq!(chunks.len(), 1);

    // 4 MB + 1 byte → two chunks
    let data2 = vec![0u8; 4_194_305];
    let chunks2 = splitter.split(&data2);
    assert_eq!(chunks2.len(), 2);
    assert_eq!(chunks2[0].1.len(), 4_194_304);
    assert_eq!(chunks2[1].1.len(), 1);
}

#[test]
fn split_10mb_into_chunks() {
    let splitter = SegmentSplitter::new(4_194_304); // 4 MB chunks
    let data = vec![0u8; 10_485_760]; // 10 MB
    let chunks = splitter.split(&data);

    assert_eq!(chunks.len(), 3); // 4 MB + 4 MB + 2 MB
    assert_eq!(chunks[0].0, 0);
    assert_eq!(chunks[0].1.len(), 4_194_304);
    assert_eq!(chunks[1].0, 4_194_304);
    assert_eq!(chunks[1].1.len(), 4_194_304);
    assert_eq!(chunks[2].0, 8_388_608);
    assert_eq!(chunks[2].1.len(), 2_097_152);
}
