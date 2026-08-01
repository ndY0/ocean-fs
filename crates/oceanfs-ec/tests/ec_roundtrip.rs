//! EC round-trip integration test.
//!
//! Encodes a full segment (4 MB) with k=4, m=2, introduces up to m
//! erasures, decodes, and verifies bit-exact recovery.

#![allow(clippy::unwrap_used)]

use oceanfs_core::CodecConfig;
use oceanfs_ec::{CauchyEncoder, Decoder, Encoder, StripeLayout};

#[test]
fn ec_roundtrip_4mb_segment_k4_m2_no_erasures() {
    let k: u8 = 4;
    let m: u8 = 4;
    let strip_size = 65536; // 64 KB
    let segment_size: u64 = 4 * 1024 * 1024; // 4 MB

    let config = CodecConfig {
        data_shards: k,
        parity_shards: m,
        strip_size_bytes: strip_size,
        ..Default::default()
    };
    let codec = CauchyEncoder::new(config);

    // Generate pseudo-random segment data (deterministic).
    let segment_data: Vec<u8> =
        (0..segment_size as usize).map(|i| ((i * 7 + 13) % 251) as u8).collect();

    let plan = StripeLayout::compute(segment_size, k, m, strip_size).unwrap();

    // Each stripe has k * strip_size bytes = 256 KB of data
    // 4 MB / 256 KB = 16 stripes
    assert_eq!(plan.stripe_count, 16);
    assert_eq!(plan.padded_size, segment_size);

    // Split into k data shards per stripe, encode parity for each.
    let data_shards: Vec<Vec<u8>> = (0..k as usize)
        .map(|shard_idx| {
            let mut shard = Vec::with_capacity(plan.stripe_count * strip_size);
            for stripe_idx in 0..plan.stripe_count {
                let start = stripe_idx * k as usize * strip_size + shard_idx * strip_size;
                let end = (start + strip_size).min(segment_data.len());
                shard.extend_from_slice(&segment_data[start..end]);
            }
            shard
        })
        .collect();

    // Encode each stripe.
    let mut parity_shards: Vec<Vec<u8>> = (0..m as usize).map(|_| Vec::new()).collect();
    for stripe_idx in 0..plan.stripe_count {
        let stripe_data: Vec<&[u8]> = data_shards
            .iter()
            .map(|shard| &shard[stripe_idx * strip_size..(stripe_idx + 1) * strip_size])
            .collect();
        let parity = codec.encode(&stripe_data, m).unwrap();
        for (p_idx, p_data) in parity.iter().enumerate() {
            parity_shards[p_idx].extend_from_slice(p_data);
        }
    }

    assert_eq!(parity_shards.len(), m as usize);
    for p in &parity_shards {
        assert_eq!(p.len(), plan.stripe_count * strip_size);
    }

    // Decode with all shards present — should recover exactly.
    let mut recovered = vec![0u8; segment_size as usize];
    for stripe_idx in 0..plan.stripe_count {
        let available: Vec<Option<&[u8]>> = {
            let mut a: Vec<Option<&[u8]>> = Vec::with_capacity((k + m) as usize);
            for shard in data_shards.iter().take(k as usize) {
                let start = stripe_idx * strip_size;
                let end = start + strip_size;
                a.push(Some(&shard[start..end]));
            }
            for shard in parity_shards.iter().take(m as usize) {
                let start = stripe_idx * strip_size;
                let end = start + strip_size;
                a.push(Some(&shard[start..end]));
            }
            a
        };

        let decoded = codec.decode(&available, k, m).unwrap();
        for (shard_idx, shard_data) in decoded.iter().enumerate() {
            let dest_start = stripe_idx * k as usize * strip_size + shard_idx * strip_size;
            let copy_len = shard_data.len().min(strip_size);
            let dest_end = (dest_start + copy_len).min(recovered.len());
            recovered[dest_start..dest_end].copy_from_slice(&shard_data[..copy_len]);
        }
    }

    assert_eq!(recovered, segment_data, "recovered data must match original exactly");
}

#[test]
fn ec_roundtrip_with_m_erasures() {
    let k: u8 = 4;
    let m: u8 = 2;
    let strip_size = 1024;
    let data_size: u64 = k as u64 * strip_size as u64; // one full stripe

    let config = CodecConfig {
        data_shards: k,
        parity_shards: m,
        strip_size_bytes: strip_size,
        ..Default::default()
    };
    let codec = CauchyEncoder::new(config);

    let segment_data: Vec<u8> =
        (0..data_size as usize).map(|i| ((i * 17 + 31) % 256) as u8).collect();

    let plan = StripeLayout::compute(data_size, k, m, strip_size).unwrap();
    assert_eq!(plan.stripe_count, 1);

    // Split into k data shards.
    let data_shards: Vec<&[u8]> = (0..k)
        .map(|i| {
            let i = i as usize;
            &segment_data[i * strip_size..(i + 1) * strip_size]
        })
        .collect();

    let parity = codec.encode(&data_shards, m).unwrap();

    // Erase data shards 0 and 1 — should recover from shards 2,3 + parity.
    let available: Vec<Option<&[u8]>> = vec![
        None, // lost
        None, // lost
        Some(data_shards[2]),
        Some(data_shards[3]),
        Some(&parity[0]),
        Some(&parity[1]),
    ];

    let recovered = codec.decode(&available, k, m).unwrap();
    assert_eq!(recovered[0], data_shards[0]);
    assert_eq!(recovered[1], data_shards[1]);
    assert_eq!(recovered[2], data_shards[2]);
    assert_eq!(recovered[3], data_shards[3]);
}

#[test]
fn ec_roundtrip_k1_m0() {
    let config = CodecConfig {
        data_shards: 1,
        parity_shards: 0,
        strip_size_bytes: 1024,
        ..Default::default()
    };
    let codec = CauchyEncoder::new(config);
    let data = vec![0x42u8; 1024];
    let parity = codec.encode(&[&data[..]], 0).unwrap();
    assert!(parity.is_empty());
}

#[test]
fn ec_roundtrip_single_byte_edge_case() {
    let config =
        CodecConfig { data_shards: 4, parity_shards: 2, strip_size_bytes: 1, ..Default::default() };
    let codec = CauchyEncoder::new(config);
    let data = [&b"a"[..], &b"b"[..], &b"c"[..], &b"d"[..]];
    let parity = codec.encode(&data, 2).unwrap();
    assert_eq!(parity.len(), 2);
    assert_eq!(parity[0].len(), 1);
}
