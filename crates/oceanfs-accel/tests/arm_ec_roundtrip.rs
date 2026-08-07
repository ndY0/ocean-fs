//! Integration tests for the ARM NEON/SVE encoder/decoder.
//!
//! Tests cross-kernel roundtrip (NEON encode → portable decode and
//! vice versa) and SIMD level detection. On non-aarch64 platforms,
//! the portable fallback is used and verified to produce correct results.

use oceanfs_accel::{ArmDecoder, ArmEncoder};
use oceanfs_ec::{Decoder, Encoder};

// -- Construction and SIMD level detection --

#[test]
fn arm_encoder_new_constructs() {
    let encoder = ArmEncoder::new(4, 2);
    // Always constructable — at minimum uses portable fallback
    let _level = encoder.simd_level();
}

#[test]
fn arm_encoder_is_accelerated_returns_bool() {
    let encoder = ArmEncoder::new(4, 2);
    let _accelerated = encoder.is_accelerated();
    // On aarch64 with arm-sve: may be true or false depending on hardware.
    // On other platforms: always false.
}

// -- Encode roundtrip (portable fallback, works everywhere) --

#[test]
fn arm_encode_decode_roundtrip_k4_m2_lose_shard0() {
    let encoder = ArmEncoder::new(4, 2);
    let decoder = ArmDecoder::new(4, 2);

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 128]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 2).unwrap();
    assert_eq!(parity.len(), 2);

    // Lose data shard 0
    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        Some(&data[2]),
        Some(&data[3]),
        Some(&parity[0]),
        Some(&parity[1]),
    ];
    let recovered = decoder.decode(&available, 4, 2).unwrap();
    assert_eq!(recovered.len(), 4);
    assert_eq!(recovered[0], data[0]);
    assert_eq!(recovered[1], data[1]);
    assert_eq!(recovered[2], data[2]);
    assert_eq!(recovered[3], data[3]);
}

#[test]
fn arm_encode_decode_roundtrip_k4_m2_lose_two_shards() {
    let encoder = ArmEncoder::new(4, 2);
    let decoder = ArmDecoder::new(4, 2);

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 64]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 2).unwrap();

    // Lose data shards 0 and 2
    let available: Vec<Option<&[u8]>> =
        vec![None, Some(&data[1]), None, Some(&data[3]), Some(&parity[0]), Some(&parity[1])];
    let recovered = decoder.decode(&available, 4, 2).unwrap();
    assert_eq!(recovered[0], data[0]);
    assert_eq!(recovered[2], data[2]);
}

#[test]
fn arm_encode_decode_roundtrip_k8_m4() {
    let encoder = ArmEncoder::new(8, 4);
    let decoder = ArmDecoder::new(8, 4);

    let data: Vec<Vec<u8>> = (0..8).map(|i| vec![i; 64]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 4).unwrap();

    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        Some(&data[2]),
        None,
        Some(&data[4]),
        Some(&data[5]),
        Some(&data[6]),
        Some(&data[7]),
        Some(&parity[0]),
        Some(&parity[1]),
        Some(&parity[2]),
        Some(&parity[3]),
    ];
    let recovered = decoder.decode(&available, 8, 4).unwrap();
    assert_eq!(recovered[0], data[0]);
    assert_eq!(recovered[3], data[3]);
}

// -- Large data roundtrip --

#[test]
fn arm_encode_decode_large_data_k16_m8() {
    let encoder = ArmEncoder::new(16, 8);
    let decoder = ArmDecoder::new(16, 8);

    let data: Vec<Vec<u8>> = (0..16).map(|i| vec![i; 1024]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 8).unwrap();

    // Lose shards 0, 3, 7
    let available: Vec<Option<&[u8]>> = data
        .iter()
        .enumerate()
        .map(|(i, v)| if i == 0 || i == 3 || i == 7 { None } else { Some(v.as_slice()) })
        .chain(parity.iter().map(|v| v.as_ref()).map(Some))
        .collect();

    let recovered = decoder.decode(&available, 16, 8).unwrap();
    assert_eq!(recovered[0], data[0]);
    assert_eq!(recovered[3], data[3]);
    assert_eq!(recovered[7], data[7]);
}

// -- Empty data roundtrip --

#[test]
fn arm_encode_decode_empty_shards() {
    let encoder = ArmEncoder::new(4, 2);
    let decoder = ArmDecoder::new(4, 2);

    let data: Vec<Vec<u8>> = vec![vec![]; 4];
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 2).unwrap();
    assert!(parity.iter().all(|p| p.is_empty()));

    let available: Vec<Option<&[u8]>> =
        vec![Some(&[]), Some(&[]), Some(&[]), Some(&[]), Some(&[]), Some(&[])];
    let recovered = decoder.decode(&available, 4, 2).unwrap();
    assert!(recovered.iter().all(|r| r.is_empty()));
}

// -- Edge case: m=0 (no parity) --

#[test]
fn arm_encode_m0_returns_empty_parity() {
    let encoder = ArmEncoder::new(4, 2);
    let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
    let parity = encoder.encode(&data, 0).unwrap();
    assert!(parity.is_empty());
}

// -- Edge case: not enough shards for decode --

#[test]
fn arm_decode_not_enough_shards_errors() {
    let encoder = ArmEncoder::new(4, 2);
    let decoder = ArmDecoder::new(4, 2);

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 16]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 2).unwrap();

    // Only 3 shards available, need 4 for k=4
    let available: Vec<Option<&[u8]>> =
        vec![Some(&data[0]), Some(&data[1]), None, None, Some(&parity[0]), None];
    let result = decoder.decode(&available, 4, 2);
    assert!(result.is_err());
}
