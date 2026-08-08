//! Write-Ahead Log configuration.
//!
//! Controls WAL directory, file rotation, and fsync batching.

use std::path::PathBuf;

/// Configuration for the Write-Ahead Log.
///
/// Controls WAL directory, file rotation, and fsync batching.
///
/// # Examples
///
/// ```
/// use oceanfs_core::WalConfig;
///
/// let config = WalConfig::default();
/// assert_eq!(config.max_file_size_bytes, 64 * 1024 * 1024);
/// ```
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory where WAL files are stored.
    pub data_dir: PathBuf,
    /// Maximum size of a single WAL file before rotation (default 64 MB).
    pub max_file_size_bytes: u64,
    /// Maximum time to wait before fsyncing a batch of WAL entries (default 5 ms).
    ///
    /// Shorter values reduce latency at the cost of more frequent fsyncs.
    pub fsync_batch_timeout_ms: u64,
    /// Whether to use `sync_file_range` + `fdatasync` instead of `sync_all`
    /// for WAL group commit on Linux (default `true` on Linux).
    ///
    /// On NVMe drives, `sync_file_range` + `fdatasync` is 2-3× faster than
    /// `sync_all` because it saves one disk barrier (inode metadata flush).
    /// Falls back to `sync_data()` on non-Linux platforms.
    pub wal_use_sync_file_range: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/oceanfs/wal"),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            wal_use_sync_file_range: cfg!(target_os = "linux"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn wal_config_defaults_are_sensible() {
        let config = WalConfig::default();
        assert_eq!(config.max_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.fsync_batch_timeout_ms, 5);
    }

    #[test]
    fn wal_use_sync_file_range_default_matches_platform() {
        let config = WalConfig::default();
        assert_eq!(config.wal_use_sync_file_range, cfg!(target_os = "linux"));
    }
}
