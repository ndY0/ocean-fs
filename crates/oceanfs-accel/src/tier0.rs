//! Tier 0: CPU SIMD backend.
//!
//! Wraps the Cauchy Reed-Solomon encoder from `oceanfs-ec`. This is the
//! fallback tier — always available, no hardware requirements beyond
//! a working CPU.
//!
//! The `CauchyEncoder` performs its own runtime SIMD dispatch (SSE4.1,
//! AVX2, AVX-512) via the GF arithmetic layer, so this wrapper does
//! not need to do any feature detection.

use oceanfs_core::CodecConfig;
use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};

/// The Tier 0 CPU SIMD EC backend.
///
/// Always available. Delegates to the portable+SIMD Cauchy RS encoder
/// in `oceanfs-ec`. This is the terminal fallback in the acceleration
/// chain — it never fails to be available.
pub(crate) struct CpuEncoder {
    inner: CauchyEncoder,
}

impl CpuEncoder {
    /// Creates a new CPU encoder with the given codec configuration.
    pub(crate) fn new(config: CodecConfig) -> Self {
        Self { inner: CauchyEncoder::new(config) }
    }
}

impl Encoder for CpuEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> oceanfs_ec::Result<Vec<Vec<u8>>> {
        self.inner.encode(data_shards, parity_count)
    }
}

impl Decoder for CpuEncoder {
    fn decode(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> oceanfs_ec::Result<Vec<Vec<u8>>> {
        self.inner.decode(available_shards, data_count, parity_count)
    }
}

/// Returns `true` if the CPU SIMD backend is available (always true).
pub(crate) fn is_cpu_available() -> bool {
    true
}

/// Returns the detected CPU SIMD capabilities as a human-readable string.
pub(crate) fn cpu_capabilities() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            "AVX-512"
        } else if std::is_x86_feature_detected!("avx2") {
            "AVX2"
        } else if std::is_x86_feature_detected!("sse4.1") {
            "SSE4.1"
        } else {
            "portable"
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        #[cfg(target_feature = "neon")]
        {
            "NEON"
        }
        #[cfg(not(target_feature = "neon"))]
        {
            "portable"
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        "portable"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn cpu_is_always_available() {
        assert!(is_cpu_available());
    }

    #[test]
    fn cpu_capabilities_is_non_empty() {
        assert!(!cpu_capabilities().is_empty());
    }

    #[test]
    fn cpu_encoder_encode_decode_roundtrip() {
        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let encoder = CpuEncoder::new(config);

        let shard_data: Vec<Vec<u8>> = (0..4).map(|i| vec![b'a' + i; 64]).collect();
        let shard_refs: Vec<&[u8]> = shard_data.iter().map(|v| v.as_slice()).collect();

        let parity = encoder.encode(&shard_refs, 2).unwrap();
        assert_eq!(parity.len(), 2);
        assert_eq!(parity[0].len(), 64);

        // Simulate losing data shard 0 — should recover it
        let available: Vec<Option<&[u8]>> = vec![
            None, // shard 0 missing
            Some(&shard_data[1]),
            Some(&shard_data[2]),
            Some(&shard_data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = encoder.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered.len(), 4);
        assert_eq!(recovered[0], shard_data[0]);
    }
}
