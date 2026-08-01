#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Stripe parallelism integration test.
//!
//! Encodes a 4 MB segment with k=4, m=2 using ParallelEncoder,
//! verifies 16 stripes produced, then decodes with 1 missing shard
//! per stripe and verifies all data recovered.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use oceanfs_core::CodecConfig;
use oceanfs_ec::{CauchyEncoder, ParallelDecoder, ParallelEncoder, StripeLayout};

#[test]
fn stripe_parallelism_4mb_k4_m2_16_stripes() {
    let k: u8 = 4;
    let m: u8 = 2;
    let strip_size = 65536; // 64 KB
    let segment_size: u64 = 4 * 1024 * 1024; // 4 MB

    let config = CodecConfig {
        data_shards: k,
        parity_shards: m,
        strip_size_bytes: strip_size,
        ..Default::default()
    };
    let codec = Arc::new(CauchyEncoder::new(config));

    let plan = StripeLayout::compute(segment_size, k, m, strip_size).unwrap();
    // 4 MB / (4 * 64 KB) = 4 MB / 256 KB = 16 stripes
    assert_eq!(plan.stripe_count, 16);
    assert_eq!(plan.data_shards, k);
    assert_eq!(plan.parity_shards, m);

    // Generate deterministic pseudo-random data.
    let segment_data: Vec<u8> = (0..segment_size as usize)
        .map(|i| ((i.wrapping_mul(13).wrapping_add(7)) % 251) as u8)
        .collect();

    let encoder = ParallelEncoder::new(codec.clone(), 0);
    let batch = encoder.encode(&segment_data, &plan).unwrap();

    // Verify correct shard counts.
    assert_eq!(batch.data.len(), k as usize);
    assert_eq!(batch.parity.len(), m as usize);

    // Each data shard has 16 stripes * 64 KB = 1 MB
    let expected_shard_size = plan.stripe_count * strip_size;
    for shard in &batch.data {
        assert_eq!(shard.len(), expected_shard_size);
    }
    for shard in &batch.parity {
        assert_eq!(shard.len(), expected_shard_size);
    }

    // Decode with data shard 0 missing from every stripe.
    let decoder = ParallelDecoder::new(codec.clone(), 0);
    let recovered = decoder.decode(&batch, &plan, &[0]).unwrap();

    assert_eq!(recovered.len(), k as usize);

    // Reassemble recovered data and compare with original.
    let mut reassembled: Vec<u8> = Vec::with_capacity(segment_size as usize);
    for stripe_idx in 0..plan.stripe_count {
        for shard in recovered.iter().take(k as usize) {
            let offset = stripe_idx * strip_size;
            let slice = &shard[offset..offset + strip_size];
            reassembled.extend_from_slice(slice);
        }
    }

    assert_eq!(reassembled, segment_data, "recovered data must match original exactly");
}

#[test]
fn stripe_parallelism_with_padding() {
    let k: u8 = 4;
    let m: u8 = 2;
    let strip_size = 1024;
    // 5000 bytes — doesn't divide evenly into stripes of 4*1024=4096 bytes.
    let data_size = 5000u64;

    let config = CodecConfig {
        data_shards: k,
        parity_shards: m,
        strip_size_bytes: strip_size,
        ..Default::default()
    };
    let codec = Arc::new(CauchyEncoder::new(config));

    let plan = StripeLayout::compute(data_size, k, m, strip_size).unwrap();
    // 5000 / 4096 = 1.22 → 2 stripes
    assert_eq!(plan.stripe_count, 2);
    assert_eq!(plan.padded_size, 2 * 4 * 1024); // 8192

    let segment_data: Vec<u8> = (0..data_size as usize).map(|i| (i % 256) as u8).collect();

    let encoder = ParallelEncoder::new(codec.clone(), 0);
    let batch = encoder.encode(&segment_data, &plan).unwrap();

    // Decode and verify original 5000 bytes are recovered.
    let decoder = ParallelDecoder::new(codec, 0);
    let recovered = decoder.decode(&batch, &plan, &[]).unwrap();

    let mut reassembled: Vec<u8> = Vec::with_capacity(data_size as usize);
    for stripe_idx in 0..plan.stripe_count {
        for shard in recovered.iter().take(k as usize) {
            let offset = stripe_idx * strip_size;
            let slice = &shard[offset..offset + strip_size];
            reassembled.extend_from_slice(slice);
        }
    }

    // Only the first `data_size` bytes should match original; trailing bytes are padding.
    assert_eq!(&reassembled[..data_size as usize], &segment_data[..]);
}

#[test]
fn stripe_parallelism_single_stripe() {
    let k: u8 = 4;
    let m: u8 = 2;
    let strip_size = 64;
    // Exactly one stripe worth of data.
    let data_size = k as usize * strip_size;

    let config = CodecConfig {
        data_shards: k,
        parity_shards: m,
        strip_size_bytes: strip_size,
        ..Default::default()
    };
    let codec = Arc::new(CauchyEncoder::new(config));

    let plan = StripeLayout::compute(data_size as u64, k, m, strip_size).unwrap();
    assert_eq!(plan.stripe_count, 1);

    let segment_data = vec![0xCCu8; data_size];

    let encoder = ParallelEncoder::new(codec.clone(), 0);
    let batch = encoder.encode(&segment_data, &plan).unwrap();

    let decoder = ParallelDecoder::new(codec, 0);
    let recovered = decoder.decode(&batch, &plan, &[]).unwrap();

    let mut reassembled = Vec::with_capacity(data_size);
    for shard in recovered.iter() {
        reassembled.extend_from_slice(shard);
    }
    assert_eq!(reassembled, segment_data);
}
