//! WAL reader — replay entries on node restart.
//!
//! Scans all WAL files in sequence, deserializes variable-length entries
//! (80-byte header + inline data), and yields them in order. Used to
//! rebuild unsealed active segments after a crash.

use std::{io::Read, path::PathBuf};

use bytes::Bytes;
use oceanfs_core::WalConfig;

use crate::{error::Result, segment::event_wal::DataWalPos, wal::entry::WalEntry};

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
        WalReplayIter { file_paths: self.files.clone(), current: 0, current_reader: None }
    }

    /// Replays all WAL entries from all files with their exact
    /// `DataWalPos` (file sequence + in-file offset) — the recovery
    /// fold's seek/sweep input (ADR-0024 Decision 2).
    ///
    /// Invalid entries are skipped and logged exactly like [`replay`](
    /// Self::replay), with the position still advancing past them so
    /// the returned positions stay exact.
    pub(crate) fn replay_positions(
        &self,
    ) -> impl Iterator<Item = Result<(DataWalPos, WalEntry)>> + '_ {
        WalReplayPosIter {
            file_paths: self.files.clone(),
            current: 0,
            current_reader: None,
            current_seq: 0,
            current_offset: 0,
        }
    }

    /// Position-yielding iteration over a single WAL file — the
    /// retention sweep's exact boundary (an entry at position `p` is
    /// garbage iff its segment's `SealEvent.data_wal_pos ≥ p`).
    pub(crate) fn entries_in_file_positions(
        path: PathBuf,
    ) -> impl Iterator<Item = Result<(DataWalPos, WalEntry)>> {
        WalReplayPosIter {
            file_paths: vec![path],
            current: 0,
            current_reader: None,
            current_seq: 0,
            current_offset: 0,
        }
    }
}

struct WalReplayIter {
    /// Remaining files to replay (owned so single-file iteration works).
    file_paths: Vec<PathBuf>,
    /// Index of the file currently being read.
    current: usize,
    current_reader: Option<std::io::BufReader<std::fs::File>>,
}

impl Iterator for WalReplayIter {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut reader) = self.current_reader {
                // Read the fixed-size header.
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
                            if self.current >= self.file_paths.len() {
                                // The truncated entry is the log TAIL:
                                // the crash (SIGKILL) cut the last write
                                // mid-record, so nothing valid can
                                // follow it. End the replay cleanly —
                                // the recovery then truncates the WAL
                                // to the last valid entry. A truncated
                                // entry with LATER files is mid-log
                                // corruption and keeps the error.
                                return None;
                            }
                            return Some(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                e,
                            )
                            .into()));
                        }

                        // Assemble full entry.
                        let mut entry = parsed;
                        entry.data = Bytes::from(data_buf);

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
            if self.current >= self.file_paths.len() {
                return None;
            }

            let path = self.file_paths[self.current].clone();
            self.current += 1;

            match std::fs::File::open(path) {
                Ok(file) => {
                    self.current_reader = Some(std::io::BufReader::new(file));
                }
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}

/// Position-yielding WAL replay iterator (the recovery fold's input).
///
/// Same skip-invalid semantics as [`WalReplayIter`], plus the exact
/// `DataWalPos` of every yielded entry: the file sequence is parsed
/// from the `wal_{seq:08}.log` file name and the offset is the
/// cumulative in-file byte position (header + data advance it).
struct WalReplayPosIter {
    /// Remaining files to replay (owned so single-file iteration works).
    file_paths: Vec<PathBuf>,
    /// Index of the file currently being read.
    current: usize,
    /// Open file being read.
    current_reader: Option<std::io::BufReader<std::fs::File>>,
    /// File sequence of the open file (parsed from its name).
    current_seq: u32,
    /// In-file byte position of the next entry (advanced past every
    /// entry read, valid or skipped — positions stay exact).
    current_offset: u64,
}

impl WalReplayPosIter {
    /// Parses the file sequence from a `wal_{seq:08}.log` name.
    fn file_seq_of(path: &std::path::Path) -> u32 {
        path.file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("wal_"))
            .and_then(|n| n.strip_suffix(".log"))
            .and_then(|n| n.parse::<u32>().ok())
            .unwrap_or(0)
    }
}

impl Iterator for WalReplayPosIter {
    type Item = Result<(DataWalPos, WalEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut reader) = self.current_reader {
                // Read the fixed-size header.
                let header_size = WalEntry::header_size();
                let mut header_buf = vec![0u8; header_size];

                match reader.read_exact(&mut header_buf) {
                    Ok(()) => {
                        // Parse the header.
                        let parsed = match WalEntry::from_header_bytes(&header_buf) {
                            Some(e) => e,
                            None => {
                                tracing::warn!("corrupted WAL entry header skipped");
                                self.current_offset += header_size as u64;
                                continue;
                            }
                        };

                        // Read the inline data.
                        let data_len = parsed.length as usize;
                        let mut data_buf = vec![0u8; data_len];
                        if let Err(e) = reader.read_exact(&mut data_buf) {
                            tracing::warn!("truncated WAL entry data: {e}");
                            if self.current >= self.file_paths.len() {
                                // Log TAIL (see WalReplayIter): the torn
                                // record ends the log — stop cleanly.
                                return None;
                            }
                            return Some(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                e,
                            )
                            .into()));
                        }

                        // Assemble full entry.
                        let mut entry = parsed;
                        entry.data = Bytes::from(data_buf);

                        if !entry.verify_crc() {
                            tracing::warn!("WAL entry CRC mismatch skipped");
                            self.current_offset += (header_size + data_len) as u64;
                            continue;
                        }

                        let pos =
                            DataWalPos { file_seq: self.current_seq, offset: self.current_offset };
                        self.current_offset += (header_size + data_len) as u64;
                        return Some(Ok((pos, entry)));
                    }
                    Err(_) => {
                        // End of file or read error — advance to next file.
                    }
                }
            }

            // Advance to the next file.
            if self.current >= self.file_paths.len() {
                return None;
            }

            let path = &self.file_paths[self.current];
            self.current_seq = Self::file_seq_of(path);
            self.current_offset = 0;
            self.current += 1;

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
            length,
            0,
            0,
            0,
            HashOutput::from_bytes([i; 32]),
            vec![i; length as usize].into(),
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
            ..Default::default()
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
            ..Default::default()
        };

        let reader = WalReader::open(&config).unwrap();
        let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>>>().unwrap();
        assert!(entries.is_empty());
    }

    /// The crash-tail fix (fleet churn): a SIGKILL mid-WAL-write tears
    /// the LAST record. The replay must end cleanly at the torn tail
    /// (the recovery then truncates the WAL) instead of hard-failing
    /// startup ("event-WAL recovery failed: failed to fill whole
    /// buffer" — node-1 could not restart after its churn kill).
    #[tokio::test]
    async fn replay_ends_cleanly_at_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            data_dir: dir.path().to_path_buf(),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        };

        {
            let writer = WalWriter::open(&config).await.unwrap();
            for i in 0..3 {
                let entry = make_test_entry(SegmentId::new(), (i * 100) as u64, 100, i as u8);
                writer.append(entry).await.unwrap();
            }
        }

        // Simulate the crash: append a torn record — a full valid
        // header claiming a 100 000-byte payload, followed by only
        // 100 bytes (the SIGKILL cut the write mid-payload).
        let wal_file = std::fs::read_dir(&config.data_dir)
            .unwrap_or_else(|_| panic!("wal dir readable: {}", config.data_dir.display()))
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "log"))
            .unwrap_or_else(|| panic!("no wal file in {}", config.data_dir.display()));
        let mut file = std::fs::OpenOptions::new().append(true).open(&wal_file).unwrap();
        let torn_header = make_test_entry(SegmentId::new(), 0, 100_000, 0xFF).to_header_bytes();
        std::io::Write::write_all(&mut file, &torn_header).unwrap();
        std::io::Write::write_all(&mut file, &[0xAA; 100]).unwrap(); // 100 of 100_000 bytes
        drop(file);

        let reader = WalReader::open(&config).unwrap();
        let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(
            entries.len(),
            3,
            "replay must yield the complete entries and stop cleanly at the torn tail"
        );
    }

    #[tokio::test]
    async fn replay_and_truncate_reads_remaining() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            data_dir: dir.path().to_path_buf(),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
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
