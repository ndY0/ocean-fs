//! RocksDB metadata store configuration.
//!
//! Controls the RocksDB block cache, memtable, per-column-family tuning,
//! and data directory for the metadata column families.
//!
//! ## RocksDB Tuning
//!
//! OceanFS's metadata workload is characterised by:
//! - **objects CF**: high write volume, point-lookup read pattern (GET/HEAD)
//! - **segments CF**: large sequential writes at seal time, occasional reads
//! - **deletions CF**: append-mostly tombstone records, low volume
//!
//! The defaults below are chosen for a metadata store up to ~100M objects
//! on a node with 8-32 GB RAM. Operators should increase `block_cache_size`
//! and `objects_write_buffer_mb` for larger deployments.

/// Configuration for the RocksDB metadata store.
///
/// # Examples
///
/// ```
/// use oceanfs_core::MetadataConfig;
///
/// let config = MetadataConfig::default();
/// assert_eq!(config.block_cache_size, 128 * 1024 * 1024);
/// assert_eq!(config.objects_write_buffer_mb, 64);
/// ```
#[derive(Debug, Clone)]
pub struct MetadataConfig {
    /// Directory for RocksDB data files.
    pub data_dir: std::path::PathBuf,
    /// Size of the RocksDB block cache in bytes (default 128 MB).
    ///
    /// A single LRU cache is shared across all three column families
    /// to avoid fragmentation and allow hot blocks from any CF to
    /// evict cold blocks. For metadata-heavy workloads (>100M objects),
    /// increase this to 512 MB or higher so the working set fits in cache.
    pub block_cache_size: usize,
    /// Size of the RocksDB memtable in bytes (default 64 MB).
    ///
    /// Used as the base for `optimize_level_style_compaction()`. Larger
    /// memtables reduce write stalls but increase memory use.
    pub memtable_size: usize,
    /// Write buffer (memtable) size for the `objects` CF in MB.
    /// Default 64 MB — balances write throughput (one PUT per object)
    /// against RAM usage.
    pub objects_write_buffer_mb: usize,
    /// Write buffer (memtable) size for the `segments` CF in MB.
    /// Default 256 MB — segment seal writes are large batches (header,
    /// index, blob references), so a larger write buffer reduces
    /// flush frequency and compaction stalls.
    pub segments_write_buffer_mb: usize,
    /// Write buffer (memtable) size for the `deletions` CF in MB.
    /// Default 16 MB — tombstone records are small and low volume,
    /// so a large write buffer would waste RAM.
    pub deletions_write_buffer_mb: usize,
    /// Maximum open files for RocksDB. Defaults to `-1` (unlimited).
    ///
    /// RocksDB manages its own file cache internally; setting unlimited
    /// avoids repeated open/close overhead for SST files. Safe for OceanFS
    /// because the total number of SST files is bounded by compaction.
    /// Operators with very large metadata stores or low `ulimit` values
    /// should cap this at 4096.
    pub max_open_files: i32,
    /// Whether to pin the RocksDB block cache in physical RAM with
    /// `mlock(2)` (default `true` on Linux).
    ///
    /// Prevents the kernel from swapping the block cache under memory
    /// pressure. Swapping the block cache is worse than OOM — it turns
    /// microsecond lookups into millisecond disk reads. Requires
    /// `CAP_IPC_LOCK` capability; if `mlock` fails for any reason
    /// (e.g., capability not held, mlock limit reached), the system
    /// logs a warning and continues without pinning.
    /// No-op on non-Linux platforms.
    pub mlock_block_cache: bool,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            data_dir: std::path::PathBuf::from("/var/lib/oceanfs/metadata"),
            block_cache_size: 128 * 1024 * 1024,
            memtable_size: 64 * 1024 * 1024,
            objects_write_buffer_mb: 64,
            segments_write_buffer_mb: 256,
            deletions_write_buffer_mb: 16,
            max_open_files: -1,
            mlock_block_cache: cfg!(target_os = "linux"),
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
        assert_eq!(config.objects_write_buffer_mb, 64);
        assert_eq!(config.segments_write_buffer_mb, 256);
        assert_eq!(config.deletions_write_buffer_mb, 16);
        assert_eq!(config.max_open_files, -1);
    }

    #[test]
    fn mlock_block_cache_default_matches_platform() {
        let config = MetadataConfig::default();
        assert_eq!(config.mlock_block_cache, cfg!(target_os = "linux"));
    }
}
