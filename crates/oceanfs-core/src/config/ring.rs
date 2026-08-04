//! Consistent hashing ring configuration.
//!
//! Configures the number of virtual nodes and replication factor
//! for the consistent hashing ring used for key-to-node routing.

/// Configuration for the consistent hashing ring.
///
/// # Examples
///
/// ```
/// use oceanfs_core::RingConfig;
///
/// let config = RingConfig::default();
/// assert_eq!(config.vnodes_per_node, 256);
/// assert_eq!(config.replication_factor, 3);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RingConfig {
    /// Number of virtual nodes per physical node (default 256).
    pub vnodes_per_node: u32,
    /// Number of successors for each key (default 3).
    pub replication_factor: u8,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self { vnodes_per_node: 256, replication_factor: 3 }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn ring_config_default_values() {
        let config = RingConfig::default();
        assert_eq!(config.vnodes_per_node, 256);
        assert_eq!(config.replication_factor, 3);
    }
}
