//! Cache eviction policy type selection.
//!
//! The [`EvictionPolicyType`] enum selects which concrete policy the
//! composition root (`oceanfs-node`) wires into the L1 object cache
//! and L2 metadata cache at startup.

/// Identifies the eviction policy to use for a cache tier.
///
/// The policy is constructed in `oceanfs-node` and injected into
/// the cache frontend (`ObjectCache`, `MetadataCache`).
///
/// # Examples
///
/// ```
/// use oceanfs_core::EvictionPolicyType;
///
/// let policy = EvictionPolicyType::Gdsf;
/// assert_eq!(format!("{policy:?}"), "Gdsf");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EvictionPolicyType {
    /// Greedy-Dual Size Frequency — size-aware, priority-based eviction.
    /// Recommended for L1 object cache with mixed-size blob workloads.
    Gdsf,
    /// Least Recently Used with Time-To-Live — staleness-deadline eviction.
    /// Recommended for L2 metadata cache where entries are uniformly small.
    TtlLru,
    /// Reserved for a future adaptive learner policy.
    /// Falls back to GDSF for L1 and TTL-LRU for L2 when selected.
    Adaptive,
}
