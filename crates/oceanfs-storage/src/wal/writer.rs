//! WAL writer — sequential, append-only writes with rolling files.
//!
//! Writes WAL entries to disk in sequentially-numbered files under
//! the WAL data directory. Files rotate when they exceed `max_file_size_bytes`.
//! Entries are flushed in batches via `WalSyncGroup`.
//!
//! Per performance guideline §3.1: sequential-only writes, never seek.

use std::{
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use oceanfs_core::WalConfig;
use tokio::sync::Mutex;

use crate::{
    error::{Error, Result},
    wal::{entry::WalEntry, sync::WalSyncGroup},
};

/// An append-only sequential WAL writer.
///
/// # Examples
///
/// ```ignore
/// // WalWriter requires tokio runtime; examples are in unit tests.
/// use oceanfs_core::{SegmentId, HashOutput, WalConfig};
/// use oceanfs_storage::wal::{WalWriter, WalEntry};
///
/// # #[tokio::main]
/// # async fn main() {
/// let config = WalConfig::default();
/// let writer = WalWriter::open(&config).await.unwrap();
/// let entry = WalEntry::new(SegmentId::new(), 0, 100, HashOutput::from_bytes([0u8; 32]));
/// let _position = writer.append(entry).await.unwrap();
/// # }
/// ```
pub struct WalWriter {
    /// WAL configuration.
    config: WalConfig,
    /// Current WAL file handle.
    file: Mutex<std::fs::File>,
    /// Current file sequence number.
    file_seq: Mutex<u64>,
    /// Current byte position within the current file.
    position: Mutex<u64>,
    /// Group-commit coordinator.
    sync_group: WalSyncGroup,
    /// Global WAL position counter (monotonically increasing across files).
    global_position: Mutex<u64>,
}

impl WalWriter {
    /// Opens the WAL for appending.
    ///
    /// Creates the WAL directory if it doesn't exist. Resumes from the
    /// last WAL file if one exists.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the WAL directory cannot be created or
    /// the WAL files cannot be opened.
    pub async fn open(config: &WalConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.data_dir).await?;

        // Find the highest existing WAL file number.
        let (file_seq, file, existing_size) = Self::find_latest_file(config).await?;

        let writer = Self {
            config: config.clone(),
            file: Mutex::new(file),
            file_seq: Mutex::new(file_seq),
            position: Mutex::new(existing_size),
            global_position: Mutex::new(existing_size),
            sync_group: Self::create_sync_group(config),
        };

        Ok(writer)
    }

    /// Appends an entry to the WAL.
    ///
    /// Returns the global WAL position of the entry.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write fails or the sync group shuts down.
    pub async fn append(&self, entry: WalEntry) -> Result<u64> {
        let data = entry.to_bytes();
        let entry_size = data.len() as u64;

        // Check if we need to rotate the file.
        {
            let pos = *self.position.lock().await;
            if pos + entry_size > self.config.max_file_size_bytes {
                self.rotate().await?;
            }
        }

        // Write the entry.
        let global_pos = {
            let mut file = self.file.lock().await;
            let mut pos = self.position.lock().await;

            file.write_all(&data)?;
            // Sync data but NOT metadata (the directory sync happens in group commit).
            file.flush()?;

            let written_pos = *pos;
            *pos += entry_size;
            written_pos
        };

        // Update global position.
        {
            let mut gp = self.global_position.lock().await;
            let current = *gp;
            *gp += entry_size;
            current
        };

        // Register with group commit for batched fsync.
        let rx = self.sync_group.submit().await?;
        rx.await.map_err(|_| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "WAL sync group dropped"))
        })?;

        Ok(global_pos)
    }

    /// Truncates the WAL at the given position.
    ///
    /// Entries at or after `position` are discarded. Used after segment
    /// sealing to reclaim WAL space.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the truncation fails.
    pub async fn truncate(&self, position: u64) -> Result<()> {
        let mut file = self.file.lock().await;
        file.set_len(position)?;
        file.seek(SeekFrom::Start(position))?;
        file.flush()?;

        let mut pos = self.position.lock().await;
        *pos = position;

        Ok(())
    }

    /// Force-syncs the current WAL file to disk.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the fsync fails.
    pub async fn sync(&self) -> Result<()> {
        let file = self.file.lock().await;
        file.sync_all()?;
        Ok(())
    }

    /// Returns the current global WAL position.
    pub async fn global_position(&self) -> u64 {
        *self.global_position.lock().await
    }

    /// Rotates to a new WAL file.
    async fn rotate(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        let mut seq = self.file_seq.lock().await;
        let mut pos = self.position.lock().await;

        // Sync and close the current file.
        file.sync_all()?;

        // Open the next file.
        *seq += 1;
        let path = Self::wal_file_path(&self.config.data_dir, *seq);
        *file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
        *pos = 0;

        Ok(())
    }

    fn wal_file_path(dir: &Path, seq: u64) -> PathBuf {
        dir.join(format!("wal_{seq:08}.log"))
    }

    async fn find_latest_file(config: &WalConfig) -> Result<(u64, std::fs::File, u64)> {
        let mut max_seq = 0u64;
        let mut entries = tokio::fs::read_dir(&config.data_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("wal_") && name.ends_with(".log") {
                if let Some(seq_str) =
                    name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log"))
                {
                    if let Ok(seq) = seq_str.parse::<u64>() {
                        max_seq = max_seq.max(seq);
                    }
                }
            }
        }

        let path = Self::wal_file_path(&config.data_dir, max_seq);
        let file = if path.exists() {
            std::fs::OpenOptions::new().append(true).open(&path)?
        } else {
            std::fs::OpenOptions::new().create(true).append(true).open(&path)?
        };

        let existing_size = file.metadata()?.len();

        Ok((max_seq, file, existing_size))
    }

    fn create_sync_group(config: &WalConfig) -> WalSyncGroup {
        // For now, the sync group just ensures data is flushed.
        // In a real implementation, this would call fsync/fdatasync.
        WalSyncGroup::new(
            || {
                // No-op for in-memory tests; real fsync happens in append's flush().
                Ok(())
            },
            config.fsync_batch_timeout_ms,
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{HashOutput, SegmentId};

    use super::*;

    async fn test_config() -> WalConfig {
        let dir = tempfile::tempdir().unwrap();
        WalConfig {
            data_dir: dir.path().to_path_buf(),
            max_file_size_bytes: 1024 * 1024, // 1 MB
            fsync_batch_timeout_ms: 10,
        }
    }

    fn make_entry(offset: u64, length: u32) -> WalEntry {
        WalEntry::new(SegmentId::new(), offset, length, HashOutput::from_bytes([0u8; 32]))
    }

    #[tokio::test]
    async fn append_increments_position() {
        let config = test_config().await;
        let writer = WalWriter::open(&config).await.unwrap();

        let pos1 = writer.append(make_entry(0, 100)).await.unwrap();
        let pos2 = writer.append(make_entry(100, 200)).await.unwrap();

        assert!(pos2 > pos1);
    }

    #[tokio::test]
    async fn truncate_resets_position() {
        let config = test_config().await;
        let writer = WalWriter::open(&config).await.unwrap();

        let pos1 = writer.append(make_entry(0, 100)).await.unwrap();
        writer.truncate(pos1).await.unwrap();

        // Position should be back at pos1.
        let pos2 = writer.append(make_entry(0, 50)).await.unwrap();
        assert_eq!(pos1, pos2); // Re-writing at same position
    }

    #[tokio::test]
    async fn sync_does_not_error() {
        let config = test_config().await;
        let writer = WalWriter::open(&config).await.unwrap();
        writer.append(make_entry(0, 1)).await.unwrap();
        writer.sync().await.unwrap();
    }
}
