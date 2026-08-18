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
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use oceanfs_core::{Counter, LabelSet, WalConfig};
use tokio::sync::Mutex;

use crate::{
    error::{Error, Result},
    segment::event_wal::DataWalPos,
    wal::{
        entry::WalEntry,
        sync::{sync_file_range_and_fdatasync, WalSyncGroup},
    },
};

/// Default maximum number of waiters per WAL fsync batch.
const DEFAULT_SYNC_BATCH_MAX_WAITERS: usize = 64;

/// Number of WAL files retained across rotations.
///
/// Rotating deletes files outside this window. The window must cover
/// every segment still unsealed at rotation time: entries for unsealed
/// segments are the only durable copy of their data (replay reads every
/// retained file). Segments fill and seal in milliseconds under load, so
/// a window of a few 64 MiB files is ample; the window also bounds disk
/// usage (64 MiB × window).
pub(crate) const WAL_RETENTION_FILES: usize = 4;

/// Rotations between deleted-segment marker prunes.
///
/// At ~10 s per rotation under load this runs every ~10 minutes — the
/// retained-file scan it performs is cheap at that cadence, and the
/// marker CF stays bounded over arbitrarily long uptimes.
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
/// let entry = WalEntry::new(SegmentId::new(), 0, 3, 3, ,0, 0, HashOutput::from_bytes([0u8; 32]), vec![1,2,3]);
/// let _position = writer.append(entry).await.unwrap();
/// # }
/// ```
pub struct WalWriter {
    /// WAL configuration.
    config: WalConfig,
    /// Metadata store handle for the deleted-segment marker pruning
    /// (the markers CF is removed in phase 3). `None` (tests, minimal
    /// embedding) skips pruning.
    /// Machine-backed retention liveness (ADR-0024 §Retention, phase 2):
    /// an entry at position `p` of segment `S` is garbage iff `S`'s
    /// `SealEvent.data_wal_pos ≥ p` (or `S` is `Deleted`). The closure is
    /// backed by the folded lifecycle registry + event positions and is
    /// set once at startup (the registry exists after the event fold).
    /// `None` falls back to the plain retention window.
    liveness:
        std::sync::OnceLock<Arc<dyn Fn(oceanfs_core::SegmentId, DataWalPos) -> bool + Send + Sync>>,
    /// Current WAL file handle (shared with sync group for true fsync).
    file: Arc<Mutex<std::fs::File>>,
    /// Current file sequence number.
    file_seq: Mutex<u64>,
    /// Current byte position within the current file.
    position: Mutex<u64>,
    /// Group-commit coordinator.
    sync_group: WalSyncGroup,
    /// Global WAL position counter (monotonically increasing across files).
    global_position: Mutex<u64>,
    /// Current file write position shared with the sync group for
    /// `sync_file_range` + `fdatasync` range tracking.
    /// Updated after every append; read by the sync group flusher.
    sync_position: Arc<AtomicU64>,
    /// Bytes written to WAL.
    bytes_written_total: Counter,
    /// WAL truncations.
    truncations_total: Counter,
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

        let file = Arc::new(Mutex::new(file));
        let sync_position = Arc::new(AtomicU64::new(existing_size));

        let sync_group = Self::create_sync_group(config, file.clone(), sync_position.clone());

        let writer = Self {
            config: config.clone(),
            liveness: std::sync::OnceLock::new(),
            file,
            file_seq: Mutex::new(file_seq),
            position: Mutex::new(existing_size),
            global_position: Mutex::new(existing_size),
            sync_group,
            sync_position,
            bytes_written_total: Counter::new(
                "wal_bytes_written_total".into(),
                "Bytes written to WAL".into(),
                LabelSet::empty(),
            ),
            truncations_total: Counter::new(
                "wal_truncations_total".into(),
                "WAL truncation operations".into(),
                LabelSet::empty(),
            ),
        };

        Ok(writer)
    }

    /// Sets the machine-backed retention liveness predicate (ADR-0024
    /// §Retention, phase 2): `true` means the entry at `pos` of the
    /// segment is garbage and does not protect its file.
    ///
    /// Called once at startup after the event fold populated the
    /// lifecycle registry (the registry does not exist when the writer
    /// is constructed). The closure is backed by the registry + event
    /// positions; the CF-derived `durable_or_deleted` scan is gone.
    /// Without it, retention falls back to the plain file-count window.
    pub fn set_liveness(
        &self,
        liveness: Arc<dyn Fn(oceanfs_core::SegmentId, DataWalPos) -> bool + Send + Sync>,
    ) {
        if self.liveness.set(liveness).is_err() {
            tracing::warn!("WAL retention liveness already set; ignoring second set");
        }
    }

    /// Appends an entry to the WAL.
    ///
    /// Returns the entry's `DataWalPos` — the (file sequence, in-file
    /// offset) pair that makes the WAL seekable (ADR-0024 Decision 2).
    /// The write path records it per segment so the lifecycle
    /// coordinator can embed the last entry's position in the
    /// `SealEvent`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write fails or the sync group shuts down.
    pub async fn append(&self, entry: WalEntry) -> Result<DataWalPos> {
        let data = entry.to_bytes();
        let entry_size = data.len() as u64;

        // Check if we need to rotate the file.
        {
            let pos = *self.position.lock().await;
            if pos + entry_size > self.config.max_file_size_bytes {
                self.rotate().await?;
            }
        }

        // Write the entry. The file sequence is read under the file lock
        // (rotation — the only writer of the sequence — needs that lock),
        // so the returned position is the entry's exact location.
        let data_wal_pos = {
            let mut file = self.file.lock().await;
            let mut pos = self.position.lock().await;
            let seq = self.file_seq.lock().await;

            file.write_all(&data)?;
            // Sync data but NOT metadata (the directory sync happens in group commit).
            file.flush()?;

            let written_pos = *pos;
            *pos += entry_size;

            // Publish the new write position so the sync group can
            // compute the byte range to flush with sync_file_range.
            self.sync_position.store(*pos, Ordering::Release);

            DataWalPos {
                file_seq: u32::try_from(*seq).map_err(|_| {
                    Error::Io(std::io::Error::other(
                        "WAL file sequence exceeded u32 (2^32 rotations)",
                    ))
                })?,
                offset: written_pos,
            }
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

        self.bytes_written_total.add(entry_size);
        Ok(data_wal_pos)
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
        self.truncations_total.inc();
        let mut file = self.file.lock().await;
        file.set_len(position)?;
        file.seek(SeekFrom::Start(position))?;
        file.flush()?;

        let mut pos = self.position.lock().await;
        *pos = position;

        // Update the sync position so the sync group knows the
        // truncated range.
        self.sync_position.store(position, Ordering::Release);

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

    /// Registers WAL counters with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        registrar.register_counter(self.bytes_written_total.clone());
        registrar.register_counter(self.truncations_total.clone());
    }

    /// Rotates to a new WAL file.
    /// Rotates to the next WAL file (internal; `pub(crate)` for tests).
    pub(crate) async fn rotate(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        let mut seq = self.file_seq.lock().await;
        let mut pos = self.position.lock().await;

        // Sync and close the current file.
        file.sync_all()?;

        // Reset the sync position — the new file starts at offset 0.
        self.sync_position.store(0, Ordering::Release);

        // Open the next file.
        *seq += 1;
        let path = Self::wal_file_path(&self.config.data_dir, *seq);
        *file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
        *pos = 0;

        // Best-effort cleanup of old rotated WAL files. The retention
        // window keeps the last few files, and the seal-aware check
        // additionally protects files holding entries for unsealed
        // segments (their only durable copy).
        super::cleanup_old_wal_files(
            &self.config,
            WAL_RETENTION_FILES,
            self.liveness.get().map(|liveness| liveness.as_ref()),
        )
        .await;

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

    fn create_sync_group(
        config: &WalConfig,
        file: Arc<Mutex<std::fs::File>>,
        sync_position: Arc<AtomicU64>,
    ) -> WalSyncGroup {
        // Group commit with async fsync closure.
        //
        // On Linux with `wal_use_sync_file_range = true`, the flusher
        // uses `sync_file_range` + `fdatasync` on the byte range written
        // since the last sync — 2-3× faster than `sync_all()` on NVMe
        // because it saves the inode metadata disk barrier.
        //
        // Falls back to `sync_all()` on non-Linux or when disabled.
        //
        // The flusher task calls this closure once per batch. On the
        // TokioFs path, we wrap the blocking call in `spawn_blocking`
        // to avoid blocking a tokio worker thread.
        //
        // On the io_uring path (`#[cfg(feature = "io-uring")]`), the
        // fsync is natively async and submitted directly via the ring.
        //
        // The closure is monomorphized at compile time — zero overhead.

        let wal_use_sync_file_range = config.wal_use_sync_file_range;

        #[cfg(not(feature = "io-uring"))]
        {
            let last_synced = Arc::new(AtomicU64::new(sync_position.load(Ordering::Acquire)));

            WalSyncGroup::new(
                move || {
                    let file = Arc::clone(&file);
                    let sync_pos = Arc::clone(&sync_position);
                    let last = Arc::clone(&last_synced);
                    async move {
                        let result = tokio::task::spawn_blocking(move || {
                            if let Ok(f) = file.try_lock() {
                                if wal_use_sync_file_range {
                                    let current = sync_pos.load(Ordering::Acquire);
                                    let prev = last.load(Ordering::Relaxed);
                                    if current > prev {
                                        // sync_file_range + fdatasync on the
                                        // dirty range since the last sync.
                                        sync_file_range_and_fdatasync(&f, prev, current - prev)
                                            .map_err(|e| {
                                                crate::error::Error::Io(std::io::Error::new(
                                                    e.kind(),
                                                    format!(
                                                        "WAL sync_file_range+fdatasync failed: {e}"
                                                    ),
                                                ))
                                            })?;
                                        last.store(current, Ordering::Release);
                                    }
                                    Ok(())
                                } else {
                                    f.sync_all().map_err(|e| {
                                        crate::error::Error::Io(std::io::Error::new(
                                            e.kind(),
                                            format!("WAL fsync failed: {e}"),
                                        ))
                                    })
                                }
                            } else {
                                // File is locked by a write — try again next batch.
                                Ok(())
                            }
                        })
                        .await;
                        match result {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(e)) => Err(e),
                            Err(join_err) => Err(crate::error::Error::Io(std::io::Error::other(
                                format!("WAL fsync join error: {join_err}"),
                            ))),
                        }
                    }
                },
                config.fsync_batch_timeout_ms,
                DEFAULT_SYNC_BATCH_MAX_WAITERS,
            )
        }

        #[cfg(feature = "io-uring")]
        {
            let last_synced = Arc::new(AtomicU64::new(sync_position.load(Ordering::Acquire)));

            WalSyncGroup::new(
                move || {
                    let file = Arc::clone(&file);
                    let sync_pos = Arc::clone(&sync_position);
                    let last = Arc::clone(&last_synced);
                    async move {
                        let result = tokio::task::spawn_blocking(move || {
                            if let Ok(f) = file.try_lock() {
                                if wal_use_sync_file_range {
                                    let current = sync_pos.load(Ordering::Acquire);
                                    let prev = last.load(Ordering::Relaxed);
                                    if current > prev {
                                        sync_file_range_and_fdatasync(&f, prev, current - prev)
                                            .map_err(|e| {
                                                crate::error::Error::Io(std::io::Error::new(
                                                    e.kind(),
                                                    format!(
                                                        "WAL sync_file_range+fdatasync failed: {e}"
                                                    ),
                                                ))
                                            })?;
                                        last.store(current, Ordering::Release);
                                    }
                                    Ok(())
                                } else {
                                    f.sync_all().map_err(|e| {
                                        crate::error::Error::Io(std::io::Error::new(
                                            e.kind(),
                                            format!("WAL fsync failed: {e}"),
                                        ))
                                    })
                                }
                            } else {
                                Ok(())
                            }
                        })
                        .await;
                        match result {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(e)) => Err(e),
                            Err(join_err) => Err(crate::error::Error::Io(std::io::Error::other(
                                format!("WAL fsync join error: {join_err}"),
                            ))),
                        }
                    }
                },
                config.fsync_batch_timeout_ms,
                DEFAULT_SYNC_BATCH_MAX_WAITERS,
            )
        }
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
            ..Default::default()
        }
    }

    fn make_entry(offset: u64, length: u32) -> WalEntry {
        WalEntry::new(
            SegmentId::new(),
            offset,
            length,
            length,
            0,
            0,
            0,
            HashOutput::from_bytes([0u8; 32]),
            vec![0u8; length as usize].into(),
        )
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
        writer.truncate(pos1.offset).await.unwrap();

        // Position should be back at pos1.
        let pos2 = writer.append(make_entry(0, 50)).await.unwrap();
        assert_eq!(pos1.offset, pos2.offset); // Re-writing at same position
        assert_eq!(pos1.file_seq, pos2.file_seq);
    }

    #[tokio::test]
    async fn sync_does_not_error() {
        let config = test_config().await;
        let writer = WalWriter::open(&config).await.unwrap();
        writer.append(make_entry(0, 1)).await.unwrap();
        writer.sync().await.unwrap();
    }
}
