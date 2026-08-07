//! WAL reader — replay entries on node restart.
//!
//! Scans all WAL files in sequence, deserializes variable-length entries
//! (80-byte header + inline data), and yields them in order. Used to
//! rebuild unsealed active segments after a crash.

use std::{io::Read, path::PathBuf};

use oceanfs_core::WalConfig;

use crate::{error::Result, wal::entry::WalEntry};

/// Reads WAL files and replays entries in order.
///
/// Uses the standard library's synchronous I/O because WAL replay
/// happens during startup, before the async runtime is fully active.
///
/// Each WAL entry is stored as an 80-byte header followed by `length`
/// bytes of inline data. The reader reads both and assembles full
/// [`WalEntry`] values for the caller.
///
/// # Examples
///
/// ```ignore
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
        let mut files = Vec::with_capacity(16);

        let dir = std::fs::read_dir(&config.data_dir)?;
        for entry in dir {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("wal_") && name.ends_with(".log") {
                files.push(entry.path());
            }
        }

        files.sort();
        Ok(Self { files })
    }

    /// Replays all WAL entries from all files.
    ///
    /// Returns an iterator over full entries (header + data). Invalid
    /// entries (wrong magic, CRC failure, truncated data) are silently
    /// skipped and logged.
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
            if let Some(ref mut reader) = self.current_reader {
                // Read the 80-byte header.
                let header_size = WalEntry::header_size();
                let mut header_buf = vec![0u8; header_size];

                match reader.read_exact(&mut header_buf) {
                    Ok(()) => {
                        // Parse the header.
                        let parsed = match WalEntry::from_header_bytes(&header_buf) {
                            Some(e) => e,
                            None => {
                                tracing::warn!("corrupted WAL entry header skipped");
                                continue;
                            }
                        };

                        // Read the inline data.
                        let data_len = parsed.length as usize;
                        let mut data_buf = vec![0u8; data_len];
                        if let Err(e) = reader.read_exact(&mut data_buf) {
                            tracing::warn!("truncated WAL entry data: {e}");
                            return Some(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                e,
                            )
                            .into()));
                        }

                        // Assemble full entry.
                        let mut entry = parsed;
                        entry.data = data_buf;

                        if !entry.verify_crc() {
                            tracing::warn!("WAL entry CRC mismatch skipped");
                            continue;
                        }

                        return Some(Ok(entry));
                    }
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

    fn make_test_entry(segment_id: SegmentId, offset: u64, length: u32, i: u8) -> WalEntry {
        WalEntry::new(
            segment_id,
            offset,
            length,
            0,
            0,
            HashOutput::from_bytes([i; 32]),
            vec![i; length as usize],
        )
    }

    async fn write_entries(config: &WalConfig, count: usize) {
        let writer = WalWriter::open(config).await.unwrap();
        for i in 0..count {
            let entry = make_test_entry(SegmentId::new(), (i * 100) as u64, 100, i as u8);
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

        {
            let writer = WalWriter::open(&config).await.unwrap();
            for i in 0..10 {
                let entry = make_test_entry(SegmentId::new(), i * 100, 100, i as u8);
                writer.append(entry).await.unwrap();
            }
        }

        let reader = WalReader::open(&config).unwrap();
        let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 10);
    }
}
