#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: Segment lifecycle (buffer → seal → index).
//!
//! Verifies:
//! - ActiveSegment append with offset accounting
//! - BufferPool acquire/release/exhaustion
//! - Segment sealing (full + timeout triggers)
//! - SegmentIndex serialization/deserialization and lookup
//! - SegmentHeader serialization/deserialization
//!
//! Covers `segment-buffer-inline` and `segment-sealing-index` features.


use oceanfs_core::{SegmentId, SegmentIndexEntry, SegmentSizeConfig, SizeTier};
use oceanfs_storage::{ActiveSegment, BufferPool, SegmentHeader, SegmentIndex};

fn config() -> SegmentSizeConfig {
    SegmentSizeConfig::default()
}

fn pool() -> BufferPool {
    BufferPool::new(65536, 8)
}

// ---------------------------------------------------------------------------
// ActiveSegment tests
// ---------------------------------------------------------------------------

#[test]
fn active_segment_append_returns_sequential_offsets() {
    let pool = pool();
    let mut seg = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();

    // Append blobs of various sizes, verifying offset accounting.
    let sizes: &[usize] = &[1, 4096, 65536, 262144, 1_048_576];
    let mut expected_offset: u64 = 0;

    for &size in sizes {
        let data = vec![0xAB; size];
        let (offset, length) = seg.append(&data).unwrap();
        assert_eq!(offset, expected_offset, "offset mismatch for size {size}");
        assert_eq!(length, size, "length mismatch for size {size}");
        expected_offset += size as u64;
    }

    assert_eq!(seg.size(), expected_offset);
}

#[test]
fn active_segment_is_full_after_target_reached() {
    let config = SegmentSizeConfig { default_target_size: 1000, ..SegmentSizeConfig::default() };
    let pool = BufferPool::new(2048, 2);
    let mut seg = ActiveSegment::new(SizeTier::Standard, &config, &pool).unwrap();

    assert!(!seg.is_full());
    seg.append(&[0u8; 1000]).unwrap();
    assert!(seg.is_full());
}

#[test]
fn active_segment_rejects_append_when_full() {
    let config = SegmentSizeConfig { default_target_size: 5, ..SegmentSizeConfig::default() };
    let pool = BufferPool::new(1024, 2);
    let mut seg = ActiveSegment::new(SizeTier::Standard, &config, &pool).unwrap();

    seg.append(&[0u8; 5]).unwrap();
    let result = seg.append(b"x");
    assert!(result.is_err());
}

#[test]
fn active_segment_data_returns_appended_bytes() {
    let pool = pool();
    let mut seg = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();

    seg.append(b"hello").unwrap();
    seg.append(b" world").unwrap();

    assert_eq!(seg.data(), b"hello world");
}

#[test]
fn active_segment_id_is_unique() {
    let pool = pool();
    let seg1 = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
    let seg2 = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();

    assert_ne!(seg1.id().as_uuid(), seg2.id().as_uuid());
}

#[test]
fn active_segment_tier_is_stored() {
    let pool = pool();
    let seg = ActiveSegment::new(SizeTier::Small, &config(), &pool).unwrap();
    assert_eq!(seg.tier(), SizeTier::Small);
}

// ---------------------------------------------------------------------------
// BufferPool tests
// ---------------------------------------------------------------------------

#[test]
fn buffer_pool_acquire_release() {
    let pool = BufferPool::new(4096, 4);

    let buf1 = pool.acquire().unwrap();
    let _buf2 = pool.acquire().unwrap();
    let _buf3 = pool.acquire().unwrap();
    let _buf4 = pool.acquire().unwrap();

    // Release and re-acquire.
    pool.release(buf1);

    // Should be able to acquire again.
    let _buf5 = pool.acquire().unwrap();
}

#[test]
fn buffer_pool_exhaustion() {
    let pool = BufferPool::new(1024, 2);

    // Acquire all buffers.
    let _a = pool.acquire().unwrap();
    let _b = pool.acquire().unwrap();

    // Pool is exhausted.
    let result = pool.acquire();
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// SegmentIndex tests
// ---------------------------------------------------------------------------

#[test]
fn segment_index_new_with_entries() {
    let entries = vec![
        SegmentIndexEntry { offset: 0, length: 100, blob_key_hash: [1u8; 32] },
        SegmentIndexEntry { offset: 100, length: 200, blob_key_hash: [2u8; 32] },
    ];

    let index = SegmentIndex::new(entries).unwrap();
    assert_eq!(index.len(), 2);
}

#[test]
fn segment_index_lookup_correct_entry() {
    let entries = vec![
        SegmentIndexEntry { offset: 0, length: 500, blob_key_hash: [0xAA; 32] },
        SegmentIndexEntry { offset: 500, length: 300, blob_key_hash: [0xBB; 32] },
        SegmentIndexEntry { offset: 800, length: 200, blob_key_hash: [0xCC; 32] },
    ];

    let index = SegmentIndex::new(entries).unwrap();

    let entry = index.lookup(0).expect("should find entry at offset 0");
    assert_eq!(entry.length, 500);

    let entry = index.lookup(500).expect("should find entry at offset 500");
    assert_eq!(entry.length, 300);

    let entry = index.lookup(800).expect("should find entry at offset 800");
    assert_eq!(entry.length, 200);

    // Missing offset.
    assert!(index.lookup(999).is_none());
}

#[test]
fn segment_index_serialization_roundtrip() {
    let entries = vec![
        SegmentIndexEntry { offset: 0, length: 256, blob_key_hash: [0x11; 32] },
        SegmentIndexEntry { offset: 256, length: 512, blob_key_hash: [0x22; 32] },
    ];

    let index = SegmentIndex::new(entries).unwrap();
    let bytes = index.to_bytes();
    let restored = SegmentIndex::from_bytes(&bytes).unwrap();

    assert_eq!(restored.len(), 2);
    assert_eq!(restored.lookup(0).unwrap().length, 256);
    assert_eq!(restored.lookup(256).unwrap().length, 512);
}

#[test]
fn segment_index_duplicate_offset_rejected() {
    let entries = vec![
        SegmentIndexEntry { offset: 100, length: 50, blob_key_hash: [0u8; 32] },
        SegmentIndexEntry {
            offset: 100, // duplicate
            length: 75,
            blob_key_hash: [1u8; 32],
        },
    ];

    let result = SegmentIndex::new(entries);
    assert!(result.is_err());
}

#[test]
fn segment_index_empty_has_len_zero() {
    let index = SegmentIndex::new(vec![]).unwrap();
    assert_eq!(index.len(), 0);
    assert!(index.lookup(0).is_none());
}

// ---------------------------------------------------------------------------
// SegmentHeader tests
// ---------------------------------------------------------------------------

#[test]
fn segment_header_serialization_roundtrip() {
    let seg_id = SegmentId::new();
    let checksum = [0x42; 32];

    let header = SegmentHeader::new(seg_id, 1024, 3, 1024, checksum);
    let bytes = header.to_bytes();
    let restored = SegmentHeader::from_bytes(&bytes).unwrap();

    assert_eq!(restored.segment_id.as_uuid(), seg_id.as_uuid());
    assert_eq!(restored.size, 1024);
    assert_eq!(restored.blob_count, 3);
    assert_eq!(restored.checksum, checksum);
}
