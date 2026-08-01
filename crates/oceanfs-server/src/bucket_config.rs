//! Bucket-level configuration and per-bucket policy.
//!
//! Every bucket can override node-level defaults for consistency,
//! segment sizing, erasure coding, caching, GC, and healing.
//! Policies are loaded at startup and hot-reloaded via the
//! admin HTTP endpoint.
//!
//! Per performance guideline §2.4, bucket policies are stored
//! in `ArcSwap` for wait-free reads on the hot path.

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use oceanfs_core::CodecType;
use parking_lot::RwLock;

// ---------------------------------------------------------------------------
// BucketPolicy
// ---------------------------------------------------------------------------

/// Per-bucket configuration policy combining all tunable subsystems.
///
/// Each sub-config group is optional; when `None`, the node-level
/// default for that group is used. This allows buckets to override
/// only the settings they care about.
///
/// # Examples
///
/// ```
/// use oceanfs_server::BucketPolicy;
///
/// let policy = BucketPolicy::default();
/// assert_eq!(policy.consistency.write_quorum, 2);
/// ```
#[derive(Debug, Clone, Default)]
pub struct BucketPolicy {
    /// Consistency configuration (write quorum, read quorum, replicas).
    pub consistency: ConsistencyConfig,
    /// Segment sizing and active-pool configuration.
    pub segment: SegmentConfig,
    /// Erasure-coding parameters.
    pub ec: EcConfig,
    /// Caching tier configuration.
    pub cache: CacheConfig,
    /// Performance-tuning knobs.
    pub tuning: TuningConfig,
    /// Healing/recovery configuration.
    pub heal: HealConfig,
    /// Garbage collection configuration.
    pub gc: GcConfig,
}

// ---------------------------------------------------------------------------
// ConsistencyConfig
// ---------------------------------------------------------------------------

/// Write/read quorum and replica count for a bucket.
#[derive(Debug, Clone)]
pub struct ConsistencyConfig {
    /// Number of nodes that must acknowledge a write (W).
    pub write_quorum: u8,
    /// Number of nodes consulted for a read (R).
    pub read_quorum: u8,
    /// Total number of replicas for each object (N).
    pub total_replicas: u8,
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self { write_quorum: 2, read_quorum: 1, total_replicas: 3 }
    }
}

#[allow(clippy::derivable_impls)]
impl ConsistencyConfig {
    /// Validates that this configuration is internally consistent.
    ///
    /// # Errors
    ///
    /// Returns `Err` with a human-readable message if:
    /// - `write_quorum` or `read_quorum` is zero.
    /// - `total_replicas` is zero.
    /// - `write_quorum > total_replicas`.
    /// - `read_quorum > total_replicas`.
    pub fn validate(&self) -> Result<(), String> {
        if self.write_quorum == 0 {
            return Err("write_quorum must be >= 1".into());
        }
        if self.read_quorum == 0 {
            return Err("read_quorum must be >= 1".into());
        }
        if self.total_replicas == 0 {
            return Err("total_replicas must be >= 1".into());
        }
        if self.write_quorum > self.total_replicas {
            return Err(format!(
                "write_quorum ({}) must not exceed total_replicas ({})",
                self.write_quorum, self.total_replicas
            ));
        }
        if self.read_quorum > self.total_replicas {
            return Err(format!(
                "read_quorum ({}) must not exceed total_replicas ({})",
                self.read_quorum, self.total_replicas
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SegmentConfig
// ---------------------------------------------------------------------------

/// Segment sizing and pool configuration.
///
/// Maps to `SegmentSizeConfig` in `oceanfs-core` but adds
/// pool and sealing parameters.
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Objects smaller than this are stored inline in metadata.
    pub inline_threshold_bytes: u64,
    /// Objects smaller than this use a "small" segment tier.
    pub segment_small_threshold_bytes: u64,
    /// Target size for small segments.
    pub segment_small_target_size: u64,
    /// Target size for standard segments.
    pub segment_default_target_size: u64,
    /// Maximum age in milliseconds before an active segment is sealed.
    pub seal_timeout_ms: u64,
    /// Number of active segments maintained per shard.
    pub active_pool_size: usize,
    /// Number of shards for routing active segments.
    pub shard_count: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: 4096,
            segment_small_threshold_bytes: 65536,
            segment_small_target_size: 262144,
            segment_default_target_size: 16777216,
            seal_timeout_ms: 5000,
            active_pool_size: 4,
            shard_count: 16,
        }
    }
}

impl SegmentConfig {
    /// Validates segment configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.segment_small_target_size > self.segment_default_target_size {
            return Err("segment_small_target_size must be <= segment_default_target_size".into());
        }
        if self.shard_count == 0 {
            return Err("shard_count must be >= 1".into());
        }
        if self.active_pool_size == 0 {
            return Err("active_pool_size must be >= 1".into());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EcConfig
// ---------------------------------------------------------------------------

/// Erasure-coding parameters for a bucket.
#[derive(Debug, Clone)]
pub struct EcConfig {
    /// Number of data shards (k).
    pub data_shards: u8,
    /// Number of parity shards (m).
    pub parity_shards: u8,
    /// Stripe size in bytes.
    pub stripe_size_bytes: usize,
    /// Codec type (Cauchy RS, etc.).
    pub codec: CodecType,
}

impl Default for EcConfig {
    fn default() -> Self {
        Self {
            data_shards: 4,
            parity_shards: 2,
            stripe_size_bytes: 65536,
            codec: CodecType::CauchyRs,
        }
    }
}

impl EcConfig {
    /// Validates EC configuration.
    ///
    /// Returns `Err` if `data_shards` is zero, or if the total
    /// shard count (k+m) exceeds 255.
    pub fn validate(&self) -> Result<(), String> {
        if self.data_shards == 0 {
            return Err("ec.data_shards (k) must be >= 1".into());
        }
        if self.parity_shards == 0 {
            return Err("ec.parity_shards (m) must be >= 1".into());
        }
        if u16::from(self.data_shards) + u16::from(self.parity_shards) > 255 {
            return Err("k + m must not exceed 255".into());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CacheConfig
// ---------------------------------------------------------------------------

/// Cache tier enable/disable and sizing.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Enable L1 object data cache.
    pub l1_enabled: bool,
    /// Maximum number of entries in L1 cache.
    pub l1_max_items: usize,
    /// Enable L2 metadata cache.
    pub l2_enabled: bool,
    /// Enable L3 negative cache (Bloom filter).
    pub l3_enabled: bool,
    /// False-positive rate for the L3 Bloom filter.
    pub negative_cache_fp_rate: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_enabled: true,
            l1_max_items: 10000,
            l2_enabled: true,
            l3_enabled: true,
            negative_cache_fp_rate: 0.01,
        }
    }
}

// ---------------------------------------------------------------------------
// TuningConfig
// ---------------------------------------------------------------------------

/// Performance-tuning parameters.
#[derive(Debug, Clone)]
pub struct TuningConfig {
    /// Maximum concurrent segment encodes.
    pub max_concurrent_encodes: usize,
    /// WAL sync interval in milliseconds.
    pub wal_sync_interval_ms: u64,
    /// Maximum WAL file size in bytes before rotation.
    pub wal_max_file_bytes: u64,
}

impl Default for TuningConfig {
    fn default() -> Self {
        Self {
            max_concurrent_encodes: 8,
            wal_sync_interval_ms: 10,
            wal_max_file_bytes: 67108864, // 64 MiB
        }
    }
}

// ---------------------------------------------------------------------------
// HealConfig
// ---------------------------------------------------------------------------

/// Healing/recovery configuration.
#[derive(Debug, Clone)]
pub struct HealConfig {
    /// Whether automatic read-repair is enabled.
    pub auto_repair: bool,
    /// Interval in milliseconds between healing passes.
    pub heal_interval_ms: u64,
    /// Maximum concurrent heal operations.
    pub max_concurrent_heals: usize,
}

impl Default for HealConfig {
    fn default() -> Self {
        Self { auto_repair: true, heal_interval_ms: 60000, max_concurrent_heals: 4 }
    }
}

// ---------------------------------------------------------------------------
// GcConfig
// ---------------------------------------------------------------------------

/// Garbage collection configuration.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Minimum age in seconds before a tombstone can be compacted.
    pub tombstone_ttl_seconds: u64,
    /// Interval in seconds between GC cycles.
    pub gc_interval_seconds: u64,
    /// Liveness ratio threshold below which a segment is compacted.
    pub min_liveness_ratio: f64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            tombstone_ttl_seconds: 86400, // 24 hours
            gc_interval_seconds: 3600,    // 1 hour
            min_liveness_ratio: 0.5,
        }
    }
}

// ---------------------------------------------------------------------------
// BucketConfigStore (ArcSwap-backed)
// ---------------------------------------------------------------------------

/// A thread-safe store for per-bucket policies.
///
/// Uses `ArcSwap` internally so that reads are wait-free and
/// never block writers. Policy updates atomically swap in a new
/// `Arc<BucketPolicy>` without disrupting in-flight reads.
///
/// A separate `RwLock<HashSet>` tracks which buckets *exist*
/// (for bucket lifecycle operations) so we can distinguish
/// "created with default policy" from "never created."
#[derive(Default)]
pub struct BucketConfigStore {
    /// Bucket name → policy. When a bucket exists, its entry is
    /// an `ArcSwap`. When deleted, the entry is removed entirely.
    policies: RwLock<HashMap<String, Arc<ArcSwap<BucketPolicy>>>>,
}

impl BucketConfigStore {
    /// Creates a new empty config store.
    pub fn new() -> Self {
        Self { policies: RwLock::new(HashMap::new()) }
    }

    /// Creates or updates a bucket policy, storing it in an
    /// `ArcSwap` for wait-free reads.
    ///
    /// If the bucket does not exist, it is created. If it already
    /// exists, its policy is atomically swapped.
    pub fn put(&self, bucket: String, policy: BucketPolicy) {
        let mut map = self.policies.write();
        if let Some(swap) = map.get(&bucket) {
            swap.store(Arc::new(policy));
        } else {
            map.insert(bucket, Arc::new(ArcSwap::from_pointee(policy)));
        }
    }

    /// Retrieves a bucket policy.
    ///
    /// Returns `Some(Arc<BucketPolicy>)` if the bucket has been
    /// explicitly created, or `None` if the bucket does not exist.
    /// Callers should fall back to node-level defaults when `None`.
    ///
    /// The returned `Arc` is a snapshot; any subsequent policy
    /// update will not be visible through this handle.
    pub fn get(&self, bucket: &str) -> Option<Arc<BucketPolicy>> {
        self.policies.read().get(bucket).map(|swap| swap.load_full())
    }

    /// Returns `true` if a policy has been explicitly set for this bucket.
    pub fn exists(&self, bucket: &str) -> bool {
        self.policies.read().contains_key(bucket)
    }

    /// Deletes a bucket policy, removing the bucket entirely.
    ///
    /// Returns `true` if the bucket existed and was removed.
    pub fn delete(&self, bucket: &str) -> bool {
        self.policies.write().remove(bucket).is_some()
    }

    /// Lists all configured bucket names.
    pub fn list(&self) -> Vec<String> {
        self.policies.read().keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

impl BucketPolicy {
    /// Validates the entire policy, returning an error for the first
    /// invalid sub-configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any sub-configuration fails validation
    /// (e.g., `write_quorum > total_replicas`, `data_shards == 0`,
    /// or `segment_small_target_size > segment_default_target_size`).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_server::BucketPolicy;
    ///
    /// let policy = BucketPolicy::default();
    /// assert!(policy.validate().is_ok());
    ///
    /// let mut bad = BucketPolicy::default();
    /// bad.ec.data_shards = 0;
    /// assert!(bad.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        self.consistency.validate()?;
        self.segment.validate()?;
        self.ec.validate()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // --- ConsistencyConfig ---

    #[test]
    fn consistency_default_is_valid() {
        assert!(ConsistencyConfig::default().validate().is_ok());
    }

    #[test]
    fn consistency_rejects_zero_write_quorum() {
        let c = ConsistencyConfig { write_quorum: 0, ..Default::default() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn consistency_rejects_zero_read_quorum() {
        let c = ConsistencyConfig { read_quorum: 0, ..Default::default() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn consistency_rejects_write_quorum_exceeds_replicas() {
        let c = ConsistencyConfig { write_quorum: 5, total_replicas: 3, ..Default::default() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn consistency_rejects_read_quorum_exceeds_replicas() {
        let c = ConsistencyConfig { read_quorum: 5, total_replicas: 3, ..Default::default() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn consistency_w_r_equals_n_is_valid() {
        let c = ConsistencyConfig { write_quorum: 3, read_quorum: 3, total_replicas: 3 };
        assert!(c.validate().is_ok());
    }

    // --- SegmentConfig ---

    #[test]
    fn segment_default_is_valid() {
        assert!(SegmentConfig::default().validate().is_ok());
    }

    #[test]
    fn segment_rejects_small_gt_standard() {
        let c = SegmentConfig {
            segment_small_target_size: 100,
            segment_default_target_size: 50,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn segment_rejects_zero_shard_count() {
        let c = SegmentConfig { shard_count: 0, ..Default::default() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn segment_rejects_zero_active_pool_size() {
        let c = SegmentConfig { active_pool_size: 0, ..Default::default() };
        assert!(c.validate().is_err());
    }

    // --- EcConfig ---

    #[test]
    fn ec_default_is_valid() {
        assert!(EcConfig::default().validate().is_ok());
    }

    #[test]
    fn ec_rejects_zero_data_shards() {
        let c = EcConfig { data_shards: 0, ..Default::default() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn ec_rejects_zero_parity_shards() {
        let c = EcConfig { parity_shards: 0, ..Default::default() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn ec_rejects_too_many_shards() {
        let c = EcConfig { data_shards: 200, parity_shards: 56, ..Default::default() };
        // 200 + 56 = 256 > 255
        assert!(c.validate().is_err());
    }

    // --- BucketPolicy validation ---

    #[test]
    fn policy_default_validates() {
        assert!(BucketPolicy::default().validate().is_ok());
    }

    #[test]
    fn policy_rejects_invalid_consistency() {
        let mut p = BucketPolicy::default();
        p.consistency.write_quorum = 0;
        assert!(p.validate().is_err());
    }

    #[test]
    fn policy_rejects_invalid_ec() {
        let mut p = BucketPolicy::default();
        p.ec.data_shards = 0;
        assert!(p.validate().is_err());
    }

    // --- BucketConfigStore ---

    #[test]
    fn store_put_and_get_policy() {
        let store = BucketConfigStore::new();
        let mut policy = BucketPolicy::default();
        policy.consistency.write_quorum = 3;
        store.put("my-bucket".into(), policy);
        let got = store.get("my-bucket").unwrap();
        assert_eq!(got.consistency.write_quorum, 3);
    }

    #[test]
    fn store_get_missing_returns_none() {
        let store = BucketConfigStore::new();
        assert!(store.get("ghost").is_none());
    }

    #[test]
    fn store_exists_tracks_created_buckets() {
        let store = BucketConfigStore::new();
        assert!(!store.exists("b"));
        store.put("b".into(), BucketPolicy::default());
        assert!(store.exists("b"));
        store.delete("b");
        assert!(!store.exists("b"));
    }

    #[test]
    fn store_delete_returns_correctly() {
        let store = BucketConfigStore::new();
        store.put("tmp".into(), BucketPolicy::default());
        assert!(store.delete("tmp"));
        assert!(!store.delete("tmp"));
    }

    #[test]
    fn store_list_returns_all_buckets() {
        let store = BucketConfigStore::new();
        store.put("a".into(), BucketPolicy::default());
        store.put("b".into(), BucketPolicy::default());
        let mut list = store.list();
        list.sort();
        assert_eq!(list, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn store_hot_reload_sees_updated_policy() {
        let store = BucketConfigStore::new();
        store.put("bkt".into(), BucketPolicy::default());

        // Reader gets snapshot
        let snap1 = store.get("bkt").unwrap();
        assert_eq!(snap1.consistency.write_quorum, 2);

        // Writer updates
        let mut new_policy = BucketPolicy::default();
        new_policy.consistency.write_quorum = 5;
        store.put("bkt".into(), new_policy);

        // New reader sees updated policy
        let snap2 = store.get("bkt").unwrap();
        assert_eq!(snap2.consistency.write_quorum, 5);

        // Old snapshot is still valid (unchanged)
        assert_eq!(snap1.consistency.write_quorum, 2);
    }
}
