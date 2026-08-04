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
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/oceanfs/wal"),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
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
}
