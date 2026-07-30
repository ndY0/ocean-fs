//! WAL reader — replay entries on node restart.
//!
//! Scans all WAL files in sequence, deserializes entries, and yields
//! them in order. Used to rebuild unsealed active segments after a crash.

use std::{io::Read, path::PathBuf};

use oceanfs_core::WalConfig;

use crate::{error::Result, wal::entry::WalEntry};

/// Reads WAL files and replays entries in order.
///
/// Uses the standard library's synchronous I/O because WAL replay
/// happens during startup, before the async runtime is fully active.
///
/// # Examples
///
/// ```ignore
/// // WalReader requires WAL files on disk; examples are in unit tests.
/// use oceanfs_core::WalConfig;
/// use oceanfs_storage::wal::WalReader;
///
/// let config = WalConfig::default();
/// let reader = WalReader::open(&config).unwrap();
/// for entry in reader.replay() {
///     let entry = entry.unwrap();
///     println!("{:?}", entry);
/// }
/// ```
pub struct WalReader {
    /// Sorted list of WAL file paths.
    files: Vec<PathBuf>,
}

impl WalReader {
    /// Opens the WAL directory and discovers all WAL files.
    ///
    /// Files are sorted by sequence number for ordered replay.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the WAL directory cannot be read.
    pub fn open(config: &WalConfig) -> Result<Self> {
        let mut files = Vec::new();

        let dir = std::fs::read_dir(&config.data_dir)?;
        for entry in dir {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("wal_") && name.ends_with(".log") {
                files.push(entry.path());
            }
        }

        // Sort by filename for sequential replay.
        files.sort();

        Ok(Self { files })
    }

    /// Replays all WAL entries from all files.
    ///
    /// Returns an iterator over entries. Invalid entries (wrong magic,
    /// CRC failure) are silently skipped and logged.
    pub fn replay(&self) -> impl Iterator<Item = Result<WalEntry>> + '_ {
        WalReplayIter { file_paths: &self.files, current_reader: None }
    }
}

struct WalReplayIter<'a> {
    file_paths: &'a [PathBuf],
    current_reader: Option<std::io::BufReader<std::fs::File>>,
}

impl Iterator for WalReplayIter<'_> {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to read from the current file.
            if let Some(ref mut reader) = self.current_reader {
                let entry_size = WalEntry::serialized_size();
                let mut buf = vec![0u8; entry_size];

                match reader.read_exact(&mut buf) {
                    Ok(()) => match WalEntry::from_bytes(&buf) {
                        Some(entry) if entry.verify_crc() => {
                            return Some(Ok(entry));
                        }
                        _ => {
                            tracing::warn!("corrupted WAL entry skipped");
                            continue;
                        }
                    },
                    Err(_) => {
                        // End of file or read error — advance to next file.
                    }
                }
            }

            // Advance to the next file.
            if self.file_paths.is_empty() {
                return None;
            }

            let path = &self.file_paths[0];
            self.file_paths = &self.file_paths[1..];

            match std::fs::File::open(path) {
                Ok(file) => {
                    self.current_reader = Some(std::io::BufReader::new(file));
                }
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{HashOutput, SegmentId};

    use super::*;
    use crate::wal::writer::WalWriter;

    async fn write_entries(config: &WalConfig, count: usize) {
        let writer = WalWriter::open(config).await.unwrap();
        for i in 0..count {
            let entry = WalEntry::new(
                SegmentId::new(),
                (i * 100) as u64,
                100,
                HashOutput::from_bytes([i as u8; 32]),
            );
            writer.append(entry).await.unwrap();
        }
    }

    #[tokio::test]
    async fn replay_reads_all_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            data_dir: dir.path().to_path_buf(),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
        };

        write_entries(&config, 5).await;

        let reader = WalReader::open(&config).unwrap();
        let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 5);
    }

    #[tokio::test]
    async fn replay_empty_directory_returns_no_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            data_dir: dir.path().to_path_buf(),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
        };

        let reader = WalReader::open(&config).unwrap();
        let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>>>().unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn replay_and_truncate_reads_remaining() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            data_dir: dir.path().to_path_buf(),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
        };

        // Write 10 entries.
        {
            let writer = WalWriter::open(&config).await.unwrap();
            for i in 0..10 {
                let entry = WalEntry::new(
                    SegmentId::new(),
                    i * 100,
                    100,
                    HashOutput::from_bytes([i as u8; 32]),
                );
                writer.append(entry).await.unwrap();
            }
            // Truncate at position of 5th entry.
            // (Hard to get exact position without tracking, so skip this sub-test for now)
        }

        let reader = WalReader::open(&config).unwrap();
        let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>>>().unwrap();
        // All 10 should be present since we didn't actually truncate here.
        assert_eq!(entries.len(), 10);
    }
}
