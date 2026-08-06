//! Per-bucket policy configuration.
//!
//! Defines replication quorum, erasure coding parameters, segment sizing
//! overrides, cache settings, and acceleration tier for individual buckets.

/// Per-bucket policy controlling replication, EC encoding, caching,
/// and acceleration behavior.
///
/// Each bucket can override node-level defaults via this policy struct.
/// Per ADR-0007, the acceleration tier is capped by the node-level
/// `CompressionConfig` ceiling.
///
/// # Examples
///
/// ```
/// use oceanfs_core::BucketPolicy;
///
/// let policy = BucketPolicy::default();
/// assert_eq!(policy.write_quorum, 2);
/// assert_eq!(policy.read_quorum, 2);
/// assert_eq!(policy.total_replicas, 3);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct BucketPolicy {
    /// Number of replica ACKs required for a write to succeed (default 2).
    pub write_quorum: u8,
    /// Number of replica ACKs required for a read to succeed (default 2).
    pub read_quorum: u8,
    /// Total number of replicas for each data shard (default 3).
    pub total_replicas: u8,
    /// Number of data shards in the EC stripe (default 4).
    pub ec_data_shards: u8,
    /// Number of parity shards in the EC stripe (default 2).
    pub ec_parity_shards: u8,
    /// EC stripe strip size in bytes (default 65536 = 64 KB).
    pub ec_strip_size_bytes: u64,
    /// EC codec name (default "cauchy_rs").
    pub ec_codec: String,
    /// Whether to enable read caching for this bucket (default true).
    pub read_cache_enabled: bool,
    /// Whether to enable write caching for this bucket (default true).
    pub write_cache_enabled: bool,
    /// Maximum object cache size in bytes (default 0 = use node default).
    pub object_cache_size_bytes: u64,
    /// Maximum metadata cache entries (default 0 = use node default).
    pub metadata_cache_entries: u64,
    /// Acceleration tier: "auto", "cpu_zstd", "cpu_igzip", "gpu_nvcomp", or "none".
    ///
    /// The effective tier is `min(bucket_tier, node_ceiling)` per ADR-0007.
    pub acceleration_tier: String,
    /// Override inline threshold in bytes (0 = use node default).
    pub inline_threshold_bytes: u64,
    /// Override small segment threshold in bytes (0 = use node default).
    pub small_threshold_bytes: u64,
    /// Override standard segment target size in bytes (0 = use node default).
    pub default_target_size: u64,
}

impl Default for BucketPolicy {
    fn default() -> Self {
        Self {
            write_quorum: 2,
            read_quorum: 2,
            total_replicas: 3,
            ec_data_shards: 4,
            ec_parity_shards: 2,
            ec_strip_size_bytes: 65536,
            ec_codec: "cauchy_rs".into(),
            read_cache_enabled: true,
            write_cache_enabled: true,
            object_cache_size_bytes: 0,
            metadata_cache_entries: 0,
            acceleration_tier: "auto".into(),
            inline_threshold_bytes: 0,
            small_threshold_bytes: 0,
            default_target_size: 0,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn bucket_policy_default_values() {
        let policy = BucketPolicy::default();
        assert_eq!(policy.write_quorum, 2);
        assert_eq!(policy.read_quorum, 2);
        assert_eq!(policy.total_replicas, 3);
        assert_eq!(policy.ec_data_shards, 4);
        assert_eq!(policy.ec_parity_shards, 2);
        assert_eq!(policy.ec_strip_size_bytes, 65536);
        assert_eq!(policy.ec_codec, "cauchy_rs");
        assert!(policy.read_cache_enabled);
        assert!(policy.write_cache_enabled);
    }

    #[test]
    fn bucket_policy_deserializes_from_toml() {
        let toml_str = r#"
            write_quorum = 3
            read_quorum = 3
            total_replicas = 5
            ec_data_shards = 6
            ec_parity_shards = 3
            ec_codec = "standard_rs"
            acceleration_tier = "cpu_zstd"
            inline_threshold_bytes = 8192
            small_threshold_bytes = 524288
            default_target_size = 8388608
        "#;
        let policy: BucketPolicy = toml::from_str(toml_str).expect("deserialize toml");
        assert_eq!(policy.write_quorum, 3);
        assert_eq!(policy.read_quorum, 3);
        assert_eq!(policy.total_replicas, 5);
        assert_eq!(policy.ec_data_shards, 6);
        assert_eq!(policy.ec_parity_shards, 3);
        assert_eq!(policy.ec_codec, "standard_rs");
        assert_eq!(policy.acceleration_tier, "cpu_zstd");
        assert_eq!(policy.inline_threshold_bytes, 8192);
        assert_eq!(policy.small_threshold_bytes, 524288);
        assert_eq!(policy.default_target_size, 8_388_608);
    }
}
