//! RocksDB metadata store configuration.
//!
//! Controls the RocksDB block cache, memtable, and data directory
//! for the metadata column families.

/// Configuration for the RocksDB metadata store.
///
/// # Examples
///
/// ```
/// use oceanfs_core::MetadataConfig;
///
/// let config = MetadataConfig::default();
/// assert_eq!(config.block_cache_size, 128 * 1024 * 1024);
/// ```
#[derive(Debug, Clone)]
pub struct MetadataConfig {
    /// Directory for RocksDB data files.
    pub data_dir: std::path::PathBuf,
    /// Size of the RocksDB block cache in bytes (default 128 MB).
    pub block_cache_size: usize,
    /// Size of the RocksDB memtable in bytes (default 64 MB).
    pub memtable_size: usize,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            data_dir: std::path::PathBuf::from("/var/lib/oceanfs/metadata"),
            block_cache_size: 128 * 1024 * 1024,
            memtable_size: 64 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn metadata_config_default_values() {
        let config = MetadataConfig::default();
        assert_eq!(config.block_cache_size, 128 * 1024 * 1024);
        assert_eq!(config.memtable_size, 64 * 1024 * 1024);
        assert_eq!(config.data_dir, std::path::PathBuf::from("/var/lib/oceanfs/metadata"));
    }
}
