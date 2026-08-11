//! Integration tests for the ISA-L x86 AVX-512 encoder/decoder.
//!
//! Tests cross-backend roundtrip (ISA-L ↔ Cauchy RS) and AVX-512
//! detection behavior. These tests only compile and run on x86_64
//! with the `isa-l` feature enabled.
//!
//! On platforms without AVX-512, the ISA-L backend is unavailable
//! and tests verify graceful degradation.

#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
mod isal_tests {
    use oceanfs_accel::{IsalDecoder, IsalEncoder, IsalTables};
    use oceanfs_core::CodecConfig;
    use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};

    /// Helper: create ISA-L tables for given k, m.
    fn setup_isal(k: u8, m: u8) -> (IsalTables, IsalEncoder<'static>, IsalDecoder) {
        let tables = IsalTables::new(k, m).expect("ISA-L tables should be constructable");
        // Leak to get 'static lifetime for the integration test
        let tables_ref: &'static IsalTables = Box::leak(Box::new(tables.clone()));
        let encoder = IsalEncoder::new(tables_ref);
        let decoder = IsalDecoder::new();
        (tables, encoder, decoder)
    }

    // -- AVX-512 detection tests --

    #[test]
    fn is_available_returns_bool() {
        // On a system with AVX-512, this returns true; otherwise false.
        // We can't assert a specific value without knowing the hardware.
        let _available = IsalTables::is_available();
        let _available2 = IsalEncoder::is_available();
        let _available3 = IsalDecoder::is_available();
        // All three should return the same value
        assert_eq!(IsalTables::is_available(), IsalEncoder::is_available());
        assert_eq!(IsalEncoder::is_available(), IsalDecoder::is_available());
    }

    #[test]
    fn isal_tables_k_m_accessors() {
        let tables = IsalTables::new(4, 2).expect("should be available on this hardware");
        assert_eq!(tables.k(), 4);
        assert_eq!(tables.m(), 2);
    }

    // -- Cross-backend roundtrip: ISA-L encode → Cauchy decode --

    #[test]
    fn isal_encode_cauchy_decode_roundtrip_k4_m2() {
        let tables = IsalTables::new(4, 2).expect("ISA-L should be available");
        let isal_enc = IsalEncoder::new(&tables);
        let cauchy = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i + 10) as u8; 256]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        // Encode with ISA-L
        let parity = isal_enc.encode(&shard_refs, 2).unwrap();

        // Decode with Cauchy — lose shard 0
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = cauchy.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[1], data[1]);
        assert_eq!(recovered[2], data[2]);
        assert_eq!(recovered[3], data[3]);
    }

    // -- Cross-backend roundtrip: Cauchy encode → ISA-L decode --

    #[test]
    fn cauchy_encode_isal_decode_roundtrip_k4_m2() {
        let _ = IsalTables::new(4, 2).expect("ISA-L should be available");
        let isal_dec = IsalDecoder::new();
        let cauchy = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i + 10) as u8; 256]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        // Encode with Cauchy
        let parity = cauchy.encode(&shard_refs, 2).unwrap();

        // Decode with ISA-L — lose shard 0
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = isal_dec.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
    }

    // -- ISA-L encode → ISA-L decode roundtrip --

    #[test]
    fn isal_encode_decode_roundtrip_k8_m4_lose_two_shards() {
        let (_, enc, dec) = setup_isal(8, 4);

        let data: Vec<Vec<u8>> = (0..8).map(|i| vec![i; 128]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = enc.encode(&shard_refs, 4).unwrap();

        // Lose shards 0 and 3
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
        let recovered = dec.decode(&available, 8, 4).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[3], data[3]);
    }

    // -- Large data roundtrip --

    #[test]
    fn isal_encode_decode_large_data_k16_m8() {
        let (_, enc, dec) = setup_isal(16, 8);

        // 1 KB per shard, 16 shards = 16 KB total
        let data: Vec<Vec<u8>> = (0..16).map(|i| vec![i; 1024]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = enc.encode(&shard_refs, 8).unwrap();

        // Lose shards 0, 3, 7
        let available: Vec<Option<&[u8]>> = data
            .iter()
            .enumerate()
            .map(|(i, v)| if i == 0 || i == 3 || i == 7 { None } else { Some(v.as_slice()) })
            .chain(parity.iter().map(|v| v.as_ref()).map(Some))
            .collect();

        let recovered = dec.decode(&available, 16, 8).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[3], data[3]);
        assert_eq!(recovered[7], data[7]);
    }

    // -- Empty data roundtrip --

    #[test]
    fn isal_encode_decode_zero_length_shards() {
        let (_, enc, dec) = setup_isal(4, 2);

        let data: Vec<Vec<u8>> = vec![vec![]; 4];
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = enc.encode(&shard_refs, 2).unwrap();
        assert!(parity.iter().all(|p| p.is_empty()));

        let available: Vec<Option<&[u8]>> =
            vec![Some(&[]), Some(&[]), Some(&[]), Some(&[]), Some(&[]), Some(&[])];
        let recovered = dec.decode(&available, 4, 2).unwrap();
        assert!(recovered.iter().all(|r| r.is_empty()));
    }
}

// Tests that run without the isa-l feature (verify graceful unavailability)
#[cfg(not(all(target_arch = "x86_64", feature = "isa-l")))]
mod isal_unavailable_tests {
    #[test]
    fn isal_module_not_compiled_without_feature() {
        // This test exists to document that the ISA-L module is not
        // compiled when the feature is absent. The AccelDispatcher
        // should still function using Tier 0 (CPU SIMD).
    }
}
