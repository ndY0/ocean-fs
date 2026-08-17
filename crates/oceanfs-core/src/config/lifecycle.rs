//! Segment lifecycle machine configuration.
//!
//! Tunes the in-memory `SegmentLifecycleRegistry` + coordinator
//! (ADR-0025 Decision 1, phase 1): the shard count of the registry map
//! and the delete-eviction grace.

/// Configuration for the segment lifecycle registry and coordinator.
///
/// The registry holds one entry per **live** segment (Reserved or
/// Sealed, not yet Deleted), sharded across
/// [`lifecycle_registry_shards`](Self::lifecycle_registry_shards)
/// `parking_lot::RwLock<HashMap>` shards. Reads (GET-path resolution,
/// GC/scrub enumeration) never block each other; writes are
/// once-per-lifecycle (fill / seal / delete).
///
/// Memory bound (ADR-0025 Decision 5 — stated at TB scale, not
/// load-test scale): ~300 B/entry × ~170K live segments/TB → ~50 MB at
/// 1 TB, ~500 MB at 10 TB (1.7M segments), ~5 GB at 100 TB. The bound
/// is O(live segments), not O(lifetime writes): `delete()` evicts.
///
/// # Examples
///
/// ```
/// use oceanfs_core::LifecycleConfig;
///
/// let config = LifecycleConfig::default();
/// assert_eq!(config.lifecycle_registry_shards, 64);
/// assert_eq!(config.delete_grace_ms, 0); // immediate eviction
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LifecycleConfig {
    /// Number of shards in the lifecycle registry map. Each shard is an
    /// independent `parking_lot::RwLock<HashMap<SegmentId, LifecycleEntry>>`;
    /// the shard for a segment is chosen by hashing its `SegmentId`.
    /// Default: 64.
    #[serde(default = "default_lifecycle_registry_shards")]
    pub lifecycle_registry_shards: usize,
    /// How long a `Deleted` entry remains in the registry before being
    /// evicted, in milliseconds. Default: 0 (immediate eviction — the
    /// registry stays O(live segments)). A non-zero grace makes the
    /// `Deleted` state observable so callers can distinguish
    /// `AlreadyDeleted` from `Missing`.
    #[serde(default = "default_delete_grace_ms")]
    pub delete_grace_ms: u64,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            lifecycle_registry_shards: default_lifecycle_registry_shards(),
            delete_grace_ms: default_delete_grace_ms(),
        }
    }
}

fn default_lifecycle_registry_shards() -> usize {
    64
}

fn default_delete_grace_ms() -> u64 {
    0
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_config_defaults_are_sane() {
        let config = LifecycleConfig::default();
        assert_eq!(config.lifecycle_registry_shards, 64);
        assert_eq!(config.delete_grace_ms, 0);
    }

    #[test]
    fn lifecycle_config_roundtrips_through_toml() {
        let config = LifecycleConfig { lifecycle_registry_shards: 128, delete_grace_ms: 250 };
        let text = toml::to_string(&config).expect("serialize");
        let parsed: LifecycleConfig = toml::from_str(&text).expect("deserialize");
        assert_eq!(parsed.lifecycle_registry_shards, 128);
        assert_eq!(parsed.delete_grace_ms, 250);
    }

    #[test]
    fn lifecycle_config_missing_fields_fall_back_to_defaults() {
        let parsed: LifecycleConfig =
            toml::from_str("").expect("empty TOML must deserialize with defaults");
        assert_eq!(parsed, LifecycleConfig::default());
    }
}
