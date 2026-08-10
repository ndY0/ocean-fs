//! Fetch strategy for controlling blob reconstruction order.
//!
//! Defines the [`FetchStrategy`] enum which determines how the read
//! coordinator prioritizes different shard sources when assembling
//! erasure-coded blobs.

/// Strategy for ordering and prioritizing blob reconstruction operations.
///
/// Applied per-bucket. The read coordinator uses this to determine
/// which shard sources to try first and whether to parallelize.
///
/// # Examples
///
/// ```
/// use oceanfs_core::FetchStrategy;
///
/// // Default is LocalFirst
/// let strategy = FetchStrategy::default();
/// assert_eq!(strategy, FetchStrategy::LocalFirst);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FetchStrategy {
    /// Local shard first, then EC reconstruction, then remote fetch.
    /// Minimizes network traffic. Default for all buckets.
    #[default]
    LocalFirst,
    /// Fetch all k+m shards in parallel, return once k arrive.
    /// Minimizes latency at the cost of network bandwidth.
    FastestK,
    /// Prefer EC reconstruction over remote fetch.
    /// Conserves bandwidth for large-object workloads.
    BandwidthOptimized,
    /// Prefer remote shard fetch over EC reconstruction.
    /// Conserves CPU for compute-bound workloads.
    CpuOptimized,
}

/// Priority ordering of shard sources during blob reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourcePriority {
    /// Local segment reader → EC recovery → remote fetch (minimize network).
    LocalEcRemote,
    /// Local segment reader → remote fetch → EC recovery (minimize CPU).
    LocalRemoteEc,
}

/// Configuration knobs driven by a [`FetchStrategy`].
///
/// Implemented by the strategy enum itself. The read coordinator
/// reads these knobs to determine fetch parallelism and completion
/// behaviour without branching on every variant.
pub trait FetchStrategyConfig {
    /// Whether to fire all shard sources concurrently via `FuturesUnordered`.
    fn parallel_fetch(&self) -> bool;
    /// Whether to return as soon as k shards arrive (vs waiting for all k+m).
    fn use_fastest_k(&self) -> bool;
    /// The order in which to try shard sources when fetching serially.
    fn source_priority(&self) -> SourcePriority;
}

impl FetchStrategyConfig for FetchStrategy {
    fn parallel_fetch(&self) -> bool {
        matches!(self, FetchStrategy::FastestK)
    }

    fn use_fastest_k(&self) -> bool {
        matches!(self, FetchStrategy::FastestK)
    }

    fn source_priority(&self) -> SourcePriority {
        match self {
            FetchStrategy::LocalFirst | FetchStrategy::BandwidthOptimized => {
                SourcePriority::LocalEcRemote
            }
            FetchStrategy::FastestK | FetchStrategy::CpuOptimized => SourcePriority::LocalRemoteEc,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_is_local_first() {
        assert_eq!(FetchStrategy::default(), FetchStrategy::LocalFirst);
    }

    #[test]
    fn serde_roundtrip_all_variants() {
        // Embed each variant in a struct for proper TOML roundtrip
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Wrapper {
            strategy: FetchStrategy,
        }

        let variants = [
            FetchStrategy::LocalFirst,
            FetchStrategy::FastestK,
            FetchStrategy::BandwidthOptimized,
            FetchStrategy::CpuOptimized,
        ];
        for &variant in &variants {
            let wrapper = Wrapper { strategy: variant };
            let toml_str = toml::to_string(&wrapper).unwrap();
            let roundtripped: Wrapper = toml::from_str(&toml_str).unwrap();
            assert_eq!(variant, roundtripped.strategy);
        }
    }

    #[test]
    fn local_first_serial() {
        let s = FetchStrategy::LocalFirst;
        assert!(!s.parallel_fetch());
        assert!(!s.use_fastest_k());
        assert_eq!(s.source_priority(), SourcePriority::LocalEcRemote);
    }

    #[test]
    fn fastest_k_parallel() {
        let s = FetchStrategy::FastestK;
        assert!(s.parallel_fetch());
        assert!(s.use_fastest_k());
    }

    #[test]
    fn bandwidth_optimized_is_local_first_alias() {
        let s = FetchStrategy::BandwidthOptimized;
        assert_eq!(s.parallel_fetch(), FetchStrategy::LocalFirst.parallel_fetch());
        assert_eq!(s.use_fastest_k(), FetchStrategy::LocalFirst.use_fastest_k());
        assert_eq!(s.source_priority(), FetchStrategy::LocalFirst.source_priority());
    }

    #[test]
    fn cpu_optimized_prefers_remote_over_ec() {
        let s = FetchStrategy::CpuOptimized;
        assert!(!s.parallel_fetch());
        assert!(!s.use_fastest_k());
        assert_eq!(s.source_priority(), SourcePriority::LocalRemoteEc);
    }

    /// T10.6: `LocalFirst` ordering prioritizes local → EC → remote.
    #[test]
    fn test_local_first_order_matches_original_behavior() {
        let s = FetchStrategy::LocalFirst;
        // LocalFirst is serial (not parallel), does not use fastest-k,
        // and prefers local then EC then remote.
        assert!(!s.parallel_fetch());
        assert!(!s.use_fastest_k());
        assert_eq!(s.source_priority(), SourcePriority::LocalEcRemote);
        // BandwidthOptimized is an alias for LocalFirst behavior.
        let bw = FetchStrategy::BandwidthOptimized;
        assert_eq!(bw.parallel_fetch(), s.parallel_fetch());
        assert_eq!(bw.use_fastest_k(), s.use_fastest_k());
        assert_eq!(bw.source_priority(), s.source_priority());
    }

    /// T10.7: `FastestK` tolerates partial failures by design — it
    /// only needs k of k+m shards to succeed.
    #[test]
    fn test_fastest_k_tolerates_partial_failures() {
        let s = FetchStrategy::FastestK;
        // FastestK is parallel and returns on k arrival, inherently
        // tolerating up to m remote failures.
        assert!(s.parallel_fetch());
        assert!(s.use_fastest_k());
        // Verify FastestK does NOT wait for all shards (k+m), only k.
        let local = FetchStrategy::LocalFirst;
        assert_ne!(s.parallel_fetch(), local.parallel_fetch());
        assert_ne!(s.use_fastest_k(), local.use_fastest_k());
    }
}
