//! Integration test: streaming EC encode through the segment pool.
//!
//! Verifies end-to-end correctness: write data through a `SegmentPool` with
//! `ec_streaming_encode = true`, extract sealed segments, and confirm that
//! streaming-computed parity shards match batch (seal-time) encode.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{CodecConfig, PoolConfig, SegmentSizeConfig, SizeTier};
use oceanfs_ec::{CauchyEncoder, Encoder};
use oceanfs_storage::{BufferPool, SegmentPool};

/// Helper: create a segment pool with streaming EC encode enabled.
fn make_streaming_pool() -> SegmentPool {
    let ec_config = CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 64,
        ..Default::default()
    };
    let pool_config = PoolConfig {
        ec_streaming_encode: true,
        active_pool_size: 1,
        shard_count: 1,
        max_inflight_encodes: 1,
        encode_queue_capacity: 4,
    };
    let size_config =
        SegmentSizeConfig { default_target_size: 1024, ..SegmentSizeConfig::default() };
    let buffer_pool = Arc::new(BufferPool::new(65536, 4));

    SegmentPool::new(pool_config, SizeTier::Standard, &size_config, buffer_pool, Some(ec_config))
        .unwrap()
}

#[test]
fn streaming_encode_single_stripe_produces_parity_in_sealing_work() {
    let pool = make_streaming_pool();

    // Write exactly one stripe: k=4, strip=64 → 256 bytes.
    let data = vec![0xABu8; 256];
    let (_seg_id, offset, length) = pool.append(&data).unwrap();
    assert_eq!(offset, 0);
    assert_eq!(length, 256);

    // Drain the seal queue — the segment is not full (target=1024 > 256),
    // so it hasn't been enqueued for sealing. Streaming encode should have
    // fired for the completed stripe though.
    //
    // Fill the rest of the segment to trigger seal.
    let remaining = 1024usize - 256;
    let fill = vec![0xCDu8; remaining];
    let _ = pool.append(&fill).unwrap();

    // Drain the seal queue.
    let mut rx = pool.take_seal_rx().expect("seal rx should be available");
    let sem = pool.seal_semaphore();

    // Wait for the seal work to appear.
    let work = rx.blocking_recv().expect("seal work should be enqueued");
    drop(sem);

    // Verify the seal work contains parity shards.
    assert!(work.parity_shards.is_some(), "streaming encode should produce parity shards");
    let parity = work.parity_shards.unwrap();
    // 4 complete stripes (1024 / 256 = 4), 2 parity shards per stripe.
    assert_eq!(parity.len(), 8, "4 stripes × 2 parity = 8 shards");

    // Verify parity shard sizes.
    for (i, shard) in parity.iter().enumerate() {
        assert_eq!(shard.len(), 64, "parity shard {i} should be 64 bytes, got {}", shard.len());
    }

    // Compare streaming parity against batch encode for stripe 0.
    let k = 4usize;
    let strip = 64;
    let data_refs: Vec<&[u8]> = (0..k).map(|i| &data[i * strip..(i + 1) * strip]).collect();
    let encoder = CauchyEncoder::new(CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 64,
        ..Default::default()
    });
    let batch_parity = encoder.encode(&data_refs, 2).unwrap();

    // Stripe 0 parity shards are the first 2 entries in the parity list.
    for i in 0..2 {
        assert_eq!(
            &parity[i][..],
            &batch_parity[i][..],
            "streaming parity shard {i} must match batch encode"
        );
    }
}

#[test]
fn streaming_encode_plain_pool_produces_no_parity() {
    let ec_config = CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 64,
        ..Default::default()
    };
    let pool_config = PoolConfig {
        ec_streaming_encode: false,
        active_pool_size: 1,
        shard_count: 1,
        max_inflight_encodes: 1,
        encode_queue_capacity: 4,
    };
    let size_config =
        SegmentSizeConfig { default_target_size: 256, ..SegmentSizeConfig::default() };
    let buffer_pool = Arc::new(BufferPool::new(65536, 4));

    let pool = SegmentPool::new(
        pool_config,
        SizeTier::Standard,
        &size_config,
        buffer_pool,
        Some(ec_config),
    )
    .unwrap();

    // Write exactly one segment worth.
    let _ = pool.append(&vec![0xEFu8; 256]).unwrap();

    let mut rx = pool.take_seal_rx().expect("seal rx should be available");
    let work = rx.blocking_recv().expect("seal work should be enqueued");

    assert!(work.parity_shards.is_none(), "plain pool should not produce parity shards");
}

#[test]
fn streaming_encode_multiple_stripes_all_encoded() {
    let pool = make_streaming_pool();

    // Write 768 bytes = 3 complete stripes (3 × 256).
    let _ = pool.append(&vec![0x11u8; 768]).unwrap();

    // Wait briefly for rayon workers to complete.
    std::thread::sleep(std::time::Duration::from_millis(20));

    // Fill the rest to trigger seal.
    let remaining = 1024usize - 768;
    let _ = pool.append(&vec![0x22u8; remaining]).unwrap();

    let mut rx = pool.take_seal_rx().expect("seal rx should be available");
    let work = rx.blocking_recv().expect("seal work should be enqueued");

    let parity = work.parity_shards.expect("streaming encode should produce parity");
    // 4 stripes × 2 parity = 8 shards.
    assert_eq!(parity.len(), 8);

    // Every parity shard should be non-empty.
    for shard in &parity {
        assert!(!shard.is_empty(), "parity shard should not be empty");
    }
}
