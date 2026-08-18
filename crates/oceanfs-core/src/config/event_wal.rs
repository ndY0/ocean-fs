//! Segment event WAL configuration (ADR-0024).
//!
//! Tunes the dedicated segment-lifecycle event log: directory, rotation
//! size, its own group-commit fsync batch window, and the byte threshold
//! that drives the checkpoint feature (`event-wal-checkpoint`).

use std::path::PathBuf;

/// Configuration for the segment event WAL (ADR-0024 Decisions 1, 3, 4).
///
/// The event log is a project-owned, append-only WAL of plain files that
/// becomes the single source of truth for segment lifecycle transitions
/// (Reserve / Seal / Delete). It has its **own** `WalSyncGroup` instance
/// (ADR-0024 Decision 4): the batch window is wider than the data WAL's
/// 5 ms default because events are sparse and a seal already pays a
/// `.dat` fsync.
///
/// Rotation is a file-size knob only; retention/truncation is the
/// checkpoint feature's job (`event_wal_checkpoint_bytes` is carried here
/// but consumed there).
///
/// # Examples
///
/// ```
/// use oceanfs_core::EventWalConfig;
///
/// let config = EventWalConfig::default();
/// assert_eq!(config.event_wal_file_size_bytes, 64 * 1024 * 1024);
/// assert_eq!(config.event_wal_fsync_batch_timeout_ms, 50);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EventWalConfig {
    /// Directory where event WAL files are stored
    /// (`{data_dir}/event-wal` by default). Files are named
    /// `evl_{seq:08}.log` and rotated at
    /// [`event_wal_file_size_bytes`](Self::event_wal_file_size_bytes).
    #[serde(default = "default_event_wal_dir")]
    pub event_wal_dir: PathBuf,
    /// Maximum size of a single event WAL file before rotation
    /// (default 64 MB). Retention of rotated files is the checkpoint
    /// feature's job — rotation never deletes.
    #[serde(default = "default_event_wal_file_size_bytes")]
    pub event_wal_file_size_bytes: u64,
    /// Maximum time in milliseconds the event log's own fsync group
    /// waits before flushing a batch (default 50 ms — wider than the
    /// data path's 5 ms, per ADR-0024 Decision 4: events are sparse, and
    /// a seal already pays a `.dat` fsync before its `SealEvent`).
    #[serde(default = "default_event_wal_fsync_batch_timeout_ms")]
    pub event_wal_fsync_batch_timeout_ms: u64,
    /// Byte threshold that triggers the event log checkpoint (default
    /// 64 MB). Consumed by the `event-wal-checkpoint` feature; the
    /// checkpoint is the only trigger — there is no time-based fallback
    /// (ADR-0024 Decision 3).
    #[serde(default = "default_event_wal_checkpoint_bytes")]
    pub event_wal_checkpoint_bytes: u64,
}

impl Default for EventWalConfig {
    fn default() -> Self {
        Self {
            event_wal_dir: default_event_wal_dir(),
            event_wal_file_size_bytes: default_event_wal_file_size_bytes(),
            event_wal_fsync_batch_timeout_ms: default_event_wal_fsync_batch_timeout_ms(),
            event_wal_checkpoint_bytes: default_event_wal_checkpoint_bytes(),
        }
    }
}

fn default_event_wal_dir() -> PathBuf {
    PathBuf::from("/var/lib/oceanfs/event-wal")
}

fn default_event_wal_file_size_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_event_wal_fsync_batch_timeout_ms() -> u64 {
    50
}

fn default_event_wal_checkpoint_bytes() -> u64 {
    64 * 1024 * 1024
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn event_wal_config_defaults_are_sane() {
        let config = EventWalConfig::default();
        assert_eq!(config.event_wal_dir, PathBuf::from("/var/lib/oceanfs/event-wal"));
        assert_eq!(config.event_wal_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.event_wal_fsync_batch_timeout_ms, 50);
        assert_eq!(config.event_wal_checkpoint_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn event_wal_config_roundtrips_through_toml() {
        let config = EventWalConfig {
            event_wal_dir: PathBuf::from("/tmp/event-wal"),
            event_wal_file_size_bytes: 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 2048,
        };
        let text = toml::to_string(&config).expect("serialize");
        let parsed: EventWalConfig = toml::from_str(&text).expect("deserialize");
        assert_eq!(parsed, config);
    }

    #[test]
    fn event_wal_config_missing_fields_fall_back_to_defaults() {
        let parsed: EventWalConfig =
            toml::from_str("").expect("empty TOML must deserialize with defaults");
        assert_eq!(parsed, EventWalConfig::default());
    }
}
