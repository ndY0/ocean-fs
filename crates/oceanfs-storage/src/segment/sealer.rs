//! Segment sealer — finalizes active segments into immutable sealed segments.
//!
//! The sealer monitors active segments for fullness or timeout, builds the
//! blob index, writes the segment to disk, truncates the WAL, and persists
//! segment metadata to the metadata store.

use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
#[cfg(test)]
use oceanfs_core::SegmentSizeConfig;
use oceanfs_core::{Counter, LabelSet, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_hash::{Blake3Hasher, Hasher};

use crate::{
    error::{Error, Result},
    io::{
        atomic_write::{create_temp, temp_path},
        direct::{DirectIoBuf, OpenOptionsDirectExt},
        segment_flush::{FinalizeOp, SegmentFlushGroup},
        IoReadMode, SegmentWriteMode,
    },
    segment::{
        buffer::ActiveSegment,
        event_wal::DataWalPos,
        handle::SegmentHandle,
        header::SegmentHeader,
        index::{SegmentIndex, SegmentIndexEntry},
        lifecycle::SegmentLifecycleCoordinator,
        parity_section::{build_parity_section, encode_segment_parity},
    },
    wal::{WalEntry, WalWriter},
};

/// Configuration for the segment sealer.
#[derive(Debug, Clone)]
pub struct SealConfig {
    /// Target size in bytes — seal when the segment exceeds this.
    pub target_size_bytes: u64,
    /// Maximum time in milliseconds before sealing a non-empty segment.
    pub seal_timeout_ms: u64,
    /// Directory where sealed segment files are written.
    pub data_dir: PathBuf,
    /// I/O read mode for segment data I/O.
    ///
    /// When `Direct`, segment data files are opened with `O_DIRECT`
    /// to bypass the OS page cache. When `Mmap`, segment files are
    /// memory-mapped for zero-copy reads. Default is `Buffered`.
    pub io_mode: IoReadMode,
    /// Write strategy for sealed segment files.
    ///
    /// `Tmpfile` uses `O_TMPFILE` + `linkat` for atomic writes
    /// (Linux 3.11+). `Rename` uses the traditional create→write→
    /// rename path. Probe once at startup with
    /// `SegmentWriteMode::probe()`.
    pub write_mode: SegmentWriteMode,
    /// Group-commit window for sealed-segment fsync, in milliseconds:
    /// how long the flush coordinator collects seal registrations
    /// before issuing the batch's per-file sync barriers (mirrors the
    /// WAL's `fsync_batch_timeout_ms`, perf rule §3.4). Default: 10 ms.
    ///
    /// Larger windows batch more concurrent seals per barrier round but
    /// add up to `fsync_batch_timeout_ms` of latency to each seal
    /// completion. The seal is asynchronous (nothing user-facing waits
    /// on it), so this is a drain-rate / burst-amortization trade-off.
    pub fsync_batch_timeout_ms: u64,
    /// Early-flush trigger: when this many seal registrations are
    /// pending, the flush coordinator flushes the batch immediately
    /// instead of waiting for the window to expire. Default: 8
    /// (matches `PoolConfig::max_inflight_encodes`).
    pub fsync_max_waiters: usize,
}

impl Default for SealConfig {
    fn default() -> Self {
        Self {
            target_size_bytes: 4 * 1024 * 1024,
            seal_timeout_ms: 5000,
            data_dir: PathBuf::new(),
            io_mode: IoReadMode::Buffered,
            write_mode: SegmentWriteMode::Rename,
            fsync_batch_timeout_ms: 10,
            fsync_max_waiters: 8,
        }
    }
}

/// Orchestrates the sealing of active segments.
///
/// The seal-complete persistence path runs through the lifecycle
/// coordinator (ADR-0025 phase 1): the flush group syncs + finalizes
/// the `.dat`, then the coordinator validates the `Reserved` entry,
/// writes the sealed metadata durably, and folds the `Sealed` state —
/// the coordinator is the only writer of segment lifecycle state.
pub struct SegmentSealer {
    config: SealConfig,
    wal: Arc<WalWriter>,
    /// Lifecycle coordinator — the single writer of segment lifecycle
    /// state (the flush group's seal batch target).
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    /// Group-commit coordinator for segment fsync + metadata batching.
    /// Lazily constructed on first seal (needs a tokio runtime).
    flush: std::sync::OnceLock<std::sync::Arc<SegmentFlushGroup>>,
    /// Segment seal error counter.
    seal_errors: Counter,
}

impl SegmentSealer {
    /// Creates a new segment sealer.
    ///
    /// `lifecycle` is the single writer of segment lifecycle state: the
    /// flush coordinator hands every batch's sealed metadata to
    /// `SegmentLifecycleCoordinator::seal_finalized_batch`, so the
    /// CF write and the registry fold stay inside
    /// `segment/lifecycle.rs`.
    pub fn new(
        config: SealConfig,
        wal: Arc<WalWriter>,
        lifecycle: Arc<SegmentLifecycleCoordinator>,
    ) -> Self {
        Self {
            config,
            wal,
            lifecycle,
            flush: std::sync::OnceLock::new(),
            seal_errors: Counter::new(
                "segment_seal_errors_total".into(),
                "Number of segment sealing failures".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Returns the flush coordinator, constructing it on first use.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context (all production
    /// call sites are async seal tasks).
    fn flush_group(&self) -> &std::sync::Arc<SegmentFlushGroup> {
        self.flush.get_or_init(|| {
            std::sync::Arc::new(SegmentFlushGroup::new(
                Arc::clone(&self.lifecycle),
                self.config.data_dir.clone(),
                self.config.fsync_batch_timeout_ms,
                self.config.fsync_max_waiters,
            ))
        })
    }

    /// Returns the seal timeout in milliseconds — the maximum time a
    /// non-empty active segment may go without being sealed (the
    /// `try_seal` timeout check; test-exercised).
    pub fn seal_timeout_ms(&self) -> u64 {
        self.config.seal_timeout_ms
    }

    /// Sets an optional blob store for unified segment data access.
    ///
    /// Attempts to seal an active segment.
    ///
    /// `entries` are the blob index entries mapping (offset, length, hash) for
    /// each blob stored in this segment. The caller (write path) computes the
    /// blob key hashes.
    ///
    /// Returns `None` if the segment is not ready to seal (not full, not timed out,
    /// or empty). Returns a `SegmentHandle` on successful seal.
    ///
    /// # Errors
    ///
    /// Returns an error if the seal process fails (disk I/O, metadata write, etc.).
    pub async fn try_seal(
        &self,
        active: &mut ActiveSegment,
        elapsed_ms: u64,
        entries: &[SegmentIndexEntry],
        merkle_root: Option<oceanfs_core::HashOutput>,
    ) -> Result<Option<SegmentHandle>> {
        // Don't seal empty segments.
        if active.size() == 0 {
            return Ok(None);
        }

        // Check seal conditions.
        let should_seal = active.is_full() || elapsed_ms >= self.config.seal_timeout_ms;
        if !should_seal {
            return Ok(None);
        }

        let result = self.seal(active, entries, merkle_root).await;
        if result.is_err() {
            self.seal_errors.inc();
        }
        result.map(Some)
    }

    /// Seals an active segment unconditionally.
    async fn seal(
        &self,
        active: &mut ActiveSegment,
        entries: &[SegmentIndexEntry],
        merkle_root: Option<oceanfs_core::HashOutput>,
    ) -> Result<SegmentHandle> {
        let segment_id = active.id();
        let tier = active.tier();
        let data = Bytes::copy_from_slice(active.data());
        self.seal_from_data(segment_id, tier, data, entries, 0, 0, 0, None, merkle_root).await
    }

    /// Seals a segment from raw data bytes, without requiring an `ActiveSegment`.
    ///
    /// This is the primary sealing entry point — works with segments that have
    /// already been extracted from the pool. Accepts the segment's identity,
    /// data bytes, tier, blob index entries, EC parameters (k, m, strip) and
    /// the seal-time Merkle root (computed by the caller over the data
    /// section with the durability crate's `MerkleTree`, 64 KiB leaves). When
    /// EC parameters are non-zero, the segment's complete stripes are encoded
    /// on the blocking pool (`spawn_blocking` — single scheduler) and the
    /// shards are persisted in a v2 parity section (with a per-shard hash
    /// table) so EC recovery can repair corrupt data shards; segments smaller
    /// than one stripe carry no parity. The Merkle root is persisted in the
    /// segment metadata so scrub and anti-entropy can verify the segment
    /// against a trusted seal-time anchor. Writes the segment file to disk,
    /// persists metadata, and truncates the WAL past the sealed boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O fails, metadata persistence fails, or
    /// WAL truncation fails.
    // Nine parameters: all are distinct pieces of the sealed segment's
    // on-disk identity (id, tier, data, blob index, EC params, parity,
    // merkle root) — bundling them would obscure the seal call sites.
    #[allow(clippy::too_many_arguments)]
    pub async fn seal_from_data(
        &self,
        segment_id: SegmentId,
        tier: SizeTier,
        data: Bytes,
        entries: &[SegmentIndexEntry],
        ec_k: u8,
        ec_m: u8,
        strip_size_bytes: usize,
        ec_encoder: Option<std::sync::Arc<dyn oceanfs_ec::Encoder>>,
        merkle_root: Option<oceanfs_core::HashOutput>,
    ) -> Result<SegmentHandle> {
        let size = data.len() as u64;
        let blob_count = entries.len() as u32;

        // Build the blob index from the provided entries.
        let index = SegmentIndex::new(entries.to_vec())?;

        // Compute the EC parity at seal time (single scheduler: the
        // CPU-bound encode runs on the blocking pool, never on the write
        // path and never on a second scheduler). The parallel encoder
        // covers every complete stripe; the tail (up to one stripe) is
        // unprotected. The parity section (v2 format) holds the shards
        // plus a per-shard hash table so the read path can locate a
        // corrupt shard precisely and reconstruct it from the others.
        let parity_bytes = if ec_k > 0 && ec_m > 0 && strip_size_bytes > 0 {
            let data_for_encode = data.clone();
            let encoded = tokio::task::spawn_blocking(move || {
                encode_segment_parity(&data_for_encode, ec_k, ec_m, strip_size_bytes, ec_encoder)
            })
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "parity encode task failed for {segment_id}: {e}"
                )))
            })?;
            match encoded {
                Some(shards) => build_parity_section(&data, ec_k, ec_m, Some(&shards)),
                None => {
                    tracing::debug!(
                        segment_id = %segment_id,
                        "no complete EC stripe; segment sealed without parity"
                    );
                    None
                }
            }
        } else {
            None
        };

        // Compute checksum. For v2 files with a parity section the
        // checksum covers data + parity section, so parity corruption is
        // detected by the read path's integrity check; v1 files hash the
        // data only.
        let checksum_bytes = if let Some(section) = parity_bytes.as_ref() {
            let mut hasher = Blake3Hasher::new();
            hasher.update(&data);
            hasher.update(section);
            let checksum = hasher.finalize();
            *checksum.as_bytes()
        } else {
            let checksum = Blake3Hasher::hash(&data);
            *checksum.as_bytes()
        };

        // The Merkle root is computed by the caller (the seal worker)
        // over the data section with the durability crate's MerkleTree;
        // it is the persisted seal-time anchor that scrub,
        // anti-entropy, and the startup incremental-tree rebuild compare
        // against. `None` when the caller did not provide one
        // (legacy/test callers).

        // Serialize header and index.
        let header = if let Some(ref section) = parity_bytes {
            let parity_offset = crate::segment::header::SEGMENT_HEADER_SIZE as u64 + size;
            SegmentHeader::with_parity(
                segment_id,
                size,
                blob_count,
                parity_offset + section.len() as u64,
                checksum_bytes,
                parity_offset,
                section.len() as u64,
            )
        } else {
            SegmentHeader::new(
                segment_id,
                size,
                blob_count,
                crate::segment::header::SEGMENT_HEADER_SIZE as u64 + size,
                checksum_bytes,
            )
        };
        let header_bytes = header.to_bytes();
        let index_bytes = index.to_bytes();

        // Write segment file: header + data + [parity] + index.
        let filename = format!("{segment_id}.dat");
        let dir = self.config.data_dir.clone();
        tokio::fs::create_dir_all(&dir).await?;

        // Metadata is built before the flush registration — the flush
        // coordinator batches it with the file's fsync (Design B), and
        // persists it through the lifecycle coordinator (validate →
        // durable → fold — the coordinator is the only writer of
        // segment lifecycle state; ADR-0025 phase 1).
        let meta = SegmentMetadata {
            segment_id,
            ec_k,
            ec_m,
            size_tier: tier,
            merkle_root,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            ),
        };

        // Design A — write/flush split: write the data to a temp file
        // (no fsync yet) on the blocking pool, then register with the
        // flush coordinator. The coordinator group-commits the per-file
        // syncs across concurrent seals and persists the metadata in one
        // RocksDB batch; the completion signal fires only after the file
        // is durable AND its metadata is written (ADR-0021 ordering —
        // the seal worker removes the sealing-data entry after
        // `seal_from_data` returns Ok).
        //
        // The file parts travel as (header, data, parity, index) slices —
        // no concatenation Vec, no data copy on the buffered path.
        let io_mode = self.config.io_mode;
        let write_mode = self.config.write_mode;
        let write_filename = filename.clone();
        let cleanup_dir = dir.clone();
        let (file, finalize_op) = tokio::task::spawn_blocking(move || {
            let parts = SegmentFileParts {
                header: &header_bytes,
                data: &data,
                parity: parity_bytes.as_deref(),
                index: &index_bytes,
            };
            write_segment_temp(&dir, &write_filename, parts, io_mode, write_mode)
        })
        .await
        .map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "segment temp write task failed for {segment_id}: {e}"
            )))
        })?
        .map_err(|e| {
            // Hygiene: if the temp write failed after creating the file,
            // remove the leftover `.tmp.{filename}` so failed seals do
            // not accumulate disk garbage (the unnamed O_TMPFILE is
            // reclaimed by the kernel on fd close).
            let _ =
                std::fs::remove_file(crate::io::atomic_write::temp_path(&cleanup_dir, &filename));
            Error::Io(std::io::Error::new(
                e.kind(),
                format!("segment write failed for {segment_id}: {e}"),
            ))
        })?;

        self.flush_group().submit(file, filename, finalize_op, meta).await?;

        // WAL entries for sealed segments are cleaned up at file rotation time.
        Ok(SegmentHandle::new(segment_id, vec![]))
    }

    /// Registers the segment sealer counters with a metrics registrar.
    ///
    /// Registers the seal-error counter plus the flush coordinator's
    /// batching counters (fsyncs, flush batches, metadata batches) so
    /// group-commit behavior is observable in production.
    pub fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        registrar.register_counter(self.seal_errors.clone());
        let stats = self.flush_group().stats();
        registrar.register_counter(stats.fsyncs_total.clone());
        registrar.register_counter(stats.batches_total.clone());
        registrar.register_counter(stats.metadata_batches_total.clone());
    }

    /// Returns a reference to the WAL writer for crash-recovery
    /// durability. Callers use this to append WAL entries alongside
    /// active segment writes.
    pub fn wal_writer(&self) -> &Arc<WalWriter> {
        &self.wal
    }

    /// Returns the directory holding sealed `.dat` files — the recovery
    /// pass's adopt probe (a durable `.dat` for a `Reserved` segment is
    /// an interrupted seal commit, crash-window row 3).
    pub(crate) fn segment_data_dir(&self) -> &std::path::Path {
        &self.config.data_dir
    }

    /// Appends a data-WAL entry and records its position with the
    /// lifecycle coordinator.
    ///
    /// This is the write path's single entry point for data-WAL appends
    /// in phase 2 (ADR-0024 Decision 2): the returned `DataWalPos` is
    /// recorded per segment so the coordinator can embed the LAST entry's
    /// position in the `SealEvent` (`data_wal_pos` correctness — the
    /// recovery fold seeks by it). The server's write path calls this
    /// instead of `wal_writer().append()`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the WAL append fails (the position is not
    /// recorded — the entry was not written).
    pub async fn append_wal_entry(&self, entry: WalEntry) -> Result<DataWalPos> {
        let segment_id = entry.segment_id();
        let pos = self.wal.append(entry).await?;
        // The coordinator is the only writer of per-segment lifecycle
        // state; the position is recorded through it, never directly on
        // the registry.
        self.lifecycle.record_data_wal_pos(segment_id, pos);
        Ok(pos)
    }
}

/// The serialized parts of a sealed segment file, in on-disk order:
/// header, data, [parity], index.
///
/// Kept as slices so the write path can emit each part directly from
/// its source buffer — the segment `Bytes` is written zero-copy (no
/// per-seal concatenation Vec, perf §1.1).
struct SegmentFileParts<'a> {
    /// Serialized segment header.
    header: &'a [u8],
    /// Raw segment data (the frozen `Bytes`).
    data: &'a [u8],
    /// Optional EC parity section.
    parity: Option<&'a [u8]>,
    /// Serialized blob index.
    index: &'a [u8],
}

impl SegmentFileParts<'_> {
    /// Total on-disk length of all parts.
    fn len(&self) -> usize {
        self.header.len() + self.data.len() + self.parity.map_or(0, |p| p.len()) + self.index.len()
    }
}

/// Writes a sealed segment's file parts to a temp file WITHOUT syncing.
///
/// No per-seal concatenation Vec is built: the buffered path writes
/// each part directly from its source slice — the segment `Bytes` is
/// written zero-copy (perf §1.1). The O_DIRECT path copies the parts
/// into ONE page-aligned buffer (alignment is unavoidable for
/// O_DIRECT), padded in place to a 512-byte multiple.
///
/// Returns the open temp file handle and the finalize operation the
/// flush coordinator must apply after the group-committed fsync:
///
/// - `io_mode == Direct` (Linux): `.tmp.{filename}` opened with
///   `O_DIRECT` (aligned buffer, 512-byte padded), finalized by rename.
///   The O_DIRECT arm now also gets its fsync via the flush coordinator
///   (previously the Direct path never synced at all — `File::flush()`
///   is a no-op).
/// - `write_mode == Tmpfile`: unnamed `O_TMPFILE`, finalized by `linkat`
///   (never visible until linked).
/// - otherwise: `.tmp.{filename}`, finalized by rename.
///
/// Runs on the blocking pool (single scheduler — the seal task never
/// performs blocking I/O on a runtime worker).
fn write_segment_temp(
    dir: &std::path::Path,
    filename: &str,
    parts: SegmentFileParts<'_>,
    io_mode: IoReadMode,
    write_mode: SegmentWriteMode,
) -> std::io::Result<(std::fs::File, FinalizeOp)> {
    if io_mode == IoReadMode::Direct {
        #[cfg(target_os = "linux")]
        {
            use std::io::Write;

            // O_DIRECT requires the buffer to be 512-byte aligned AND
            // the I/O size to be a multiple of 512 bytes. Build ONE
            // aligned buffer from the parts and pad in place.
            const BLOCK_SIZE: usize = 512;
            let total = parts.len();
            let pad = (BLOCK_SIZE - (total % BLOCK_SIZE)) % BLOCK_SIZE;

            let mut aligned = DirectIoBuf::new(total + pad)?;
            let buf = aligned.as_bytes_mut();
            let mut off = 0;
            buf[off..off + parts.header.len()].copy_from_slice(parts.header);
            off += parts.header.len();
            buf[off..off + parts.data.len()].copy_from_slice(parts.data);
            off += parts.data.len();
            if let Some(p) = parts.parity {
                buf[off..off + p.len()].copy_from_slice(p);
                off += p.len();
            }
            buf[off..off + parts.index.len()].copy_from_slice(parts.index);
            // `pad` bytes remain zero (DirectIoBuf is zero-initialised).

            let tmp = temp_path(dir, filename);
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .with_direct()
                .open(&tmp)?;
            file.write_all(aligned.as_bytes())?;
            return Ok((file, FinalizeOp::Rename));
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = write_mode;
            // O_DIRECT is not available — fall through to the buffered
            // temp-file path (rename finalize).
        }
    }

    let file = create_temp(write_mode, dir, filename)?;
    {
        use std::io::Write;
        // Zero-copy: write each part directly from its source slice.
        (&file).write_all(parts.header)?;
        (&file).write_all(parts.data)?;
        if let Some(p) = parts.parity {
            (&file).write_all(p)?;
        }
        (&file).write_all(parts.index)?;
    }
    let op = match write_mode {
        SegmentWriteMode::Tmpfile => FinalizeOp::Link,
        SegmentWriteMode::Rename => FinalizeOp::Rename,
    };
    Ok((file, op))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::needless_range_loop)]
mod tests {
    use oceanfs_core::{HashOutput, LifecycleConfig, WalConfig};

    use super::*;
    use crate::{buffer_pool::BufferPool, segment::lifecycle::SegmentLifecycleCoordinator};

    /// Creates the sealer plus its lifecycle coordinator. Every seal in
    /// these tests reserves its segment FIRST — the flush path
    /// validates `Reserved`-only (ADR-0025: seal is Reserved-only), so
    /// an unreserved seal is rejected by construction.
    async fn setup() -> (
        SegmentSealer,
        Arc<SegmentLifecycleCoordinator>,
        ActiveSegment,
        Vec<SegmentIndexEntry>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();

        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::new(&LifecycleConfig::default()).with_event_wal(Arc::new(
                crate::segment::event_wal::EventWal::open(
                    dir.path().join("event-wal"),
                    &oceanfs_core::EventWalConfig {
                        event_wal_dir: dir.path().join("event-wal"),
                        event_wal_file_size_bytes: 1024 * 1024,
                        event_wal_fsync_batch_timeout_ms: 10,
                        event_wal_checkpoint_bytes: 1024 * 1024,
                    },
                )
                .await
                .unwrap(),
            )),
        );

        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let config = SealConfig {
            target_size_bytes: 100,
            seal_timeout_ms: 1000,
            data_dir: dir.path().join("segments"),
            io_mode: IoReadMode::Buffered,
            write_mode: SegmentWriteMode::Rename,
            ..Default::default()
        };

        let pool = BufferPool::new(65536, 4);
        let size_config =
            SegmentSizeConfig { default_target_size: 100, ..SegmentSizeConfig::default() };
        let mut active = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).unwrap();

        // Write some data so it's not empty.
        active.append(&[0u8; 50]).unwrap();

        // Build an index entry covering the appended data.
        let entries = vec![SegmentIndexEntry { offset: 0, length: 50, blob_key_hash: [0xAB; 32] }];

        let sealer = SegmentSealer::new(config, wal, Arc::clone(&lifecycle));
        (sealer, lifecycle, active, entries, dir)
    }

    #[tokio::test]
    async fn try_seal_returns_none_when_not_full_and_not_timed_out() {
        let (sealer, _lifecycle, mut active, entries, _dir) = setup().await;
        let result = sealer
            .try_seal(&mut active, 0, &entries, Some(HashOutput::from_bytes([0xAB; 32])))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn try_seal_returns_handle_when_full() {
        let (sealer, lifecycle, mut active, entries, _dir) = setup().await;
        // Fill it up.
        active.append(&[0u8; 60]).unwrap();
        assert!(active.is_full());
        // The seal path validates Reserved-only — reserve first.
        lifecycle.request_reserve(active.id(), SizeTier::Standard, 0, 0).await.unwrap();

        let result = sealer
            .try_seal(&mut active, 0, &entries, Some(HashOutput::from_bytes([0xAB; 32])))
            .await
            .unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn try_seal_returns_handle_when_timed_out() {
        let (sealer, lifecycle, mut active, entries, _dir) = setup().await;
        // Not full, but timed out.
        lifecycle.request_reserve(active.id(), SizeTier::Standard, 0, 0).await.unwrap();
        let result = sealer
            .try_seal(&mut active, 2000, &entries, Some(HashOutput::from_bytes([0xAB; 32])))
            .await
            .unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn try_seal_returns_none_for_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::new(&LifecycleConfig::default()).with_event_wal(Arc::new(
                crate::segment::event_wal::EventWal::open(
                    dir.path().join("event-wal"),
                    &oceanfs_core::EventWalConfig {
                        event_wal_dir: dir.path().join("event-wal"),
                        event_wal_file_size_bytes: 1024 * 1024,
                        event_wal_fsync_batch_timeout_ms: 10,
                        event_wal_checkpoint_bytes: 1024 * 1024,
                    },
                )
                .await
                .unwrap(),
            )),
        );
        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let config = SealConfig {
            target_size_bytes: 100,
            seal_timeout_ms: 1000,
            data_dir: dir.path().join("segments"),
            io_mode: IoReadMode::Buffered,
            write_mode: SegmentWriteMode::Rename,
            ..Default::default()
        };
        let pool = BufferPool::new(65536, 4);
        let size_config =
            SegmentSizeConfig { default_target_size: 100, ..SegmentSizeConfig::default() };
        let mut active = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).unwrap();
        let sealer = SegmentSealer::new(config, wal, lifecycle);

        // Empty segment should not seal.
        let result = sealer
            .try_seal(&mut active, 2000, &[], Some(HashOutput::from_bytes([0xAB; 32])))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    // --- Metrics tests ---

    #[tokio::test]
    async fn register_metrics_registers_seal_errors() {
        use oceanfs_core::MetricRegistrar;

        struct TestRegistrar {
            counter_names: parking_lot::Mutex<Vec<String>>,
        }
        impl MetricRegistrar for TestRegistrar {
            fn register_counter(&self, counter: oceanfs_core::Counter) {
                self.counter_names.lock().push(counter.name().to_string());
            }
            fn register_gauge(&self, _: oceanfs_core::Gauge) {}
            fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
        }

        let (sealer, _lifecycle, _active, _entries, _dir) = setup().await;
        let reg = TestRegistrar { counter_names: parking_lot::Mutex::new(Vec::new()) };

        sealer.register_metrics(&reg);

        let names = reg.counter_names.lock();
        assert!(
            names.contains(&"segment_seal_errors_total".to_string()),
            "seal_errors counter should be registered, got: {names:?}"
        );
        // The flush coordinator's batching counters must be registered
        // too (metrics-counter instrumentation for the group commit).
        assert!(
            names.contains(&"segment_fsyncs_total".to_string()),
            "fsyncs counter should be registered, got: {names:?}"
        );
        assert!(
            names.contains(&"segment_metadata_batches_total".to_string()),
            "metadata batch counter should be registered, got: {names:?}"
        );
    }

    #[tokio::test]
    async fn concurrent_seals_group_commit_fsyncs_and_batch_metadata() {
        use oceanfs_core::{SegmentId, SizeTier};

        use crate::io::segment_flush::LAST_FLUSH_THREAD;

        let dir = tempfile::tempdir().unwrap();
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::new(&LifecycleConfig::default()).with_event_wal(Arc::new(
                crate::segment::event_wal::EventWal::open(
                    dir.path().join("event-wal"),
                    &oceanfs_core::EventWalConfig {
                        event_wal_dir: dir.path().join("event-wal"),
                        event_wal_file_size_bytes: 1024 * 1024,
                        event_wal_fsync_batch_timeout_ms: 10,
                        event_wal_checkpoint_bytes: 1024 * 1024,
                    },
                )
                .await
                .unwrap(),
            )),
        );
        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        // Small window + small max_waiters so the batch trigger is
        // exercised deterministically: 16 seals → at most 2 flush
        // batches (max_waiters = 8).
        let config = SealConfig {
            target_size_bytes: 4096,
            seal_timeout_ms: 1000,
            data_dir: dir.path().join("segments"),
            io_mode: IoReadMode::Buffered,
            write_mode: SegmentWriteMode::Rename,
            fsync_batch_timeout_ms: 100,
            fsync_max_waiters: 8,
        };
        let sealer = Arc::new(SegmentSealer::new(config, wal, Arc::clone(&lifecycle)));

        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        let test_thread = hasher.finish();

        // 16 concurrent seals, each 2 KiB of data (one stripe-less
        // standard segment with a single index entry).
        let mut handles = Vec::new();
        for _ in 0..16 {
            let sealer = Arc::clone(&sealer);
            let lifecycle = Arc::clone(&lifecycle);
            handles.push(tokio::spawn(async move {
                let id = SegmentId::new();
                // Reserve first — the flush path seals Reserved-only.
                lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
                let data = Bytes::from(vec![0x5Au8; 2048]);
                let entries =
                    vec![SegmentIndexEntry { offset: 0, length: 2048, blob_key_hash: [0x11; 32] }];
                sealer
                    .seal_from_data(
                        id,
                        SizeTier::Standard,
                        data,
                        &entries,
                        0,
                        0,
                        0,
                        None,
                        Some(HashOutput::from_bytes([0xAB; 32])),
                    )
                    .await
                    .expect("seal must succeed");
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // The fsync count is one per file (syscalls cannot be batched),
        // but the syncs must run on the flush coordinator's blocking
        // pool — never on a seal task's runtime worker — and the
        // metadata writes must be batched: 16 seals with max_waiters=8
        // → at most 2 RocksDB WriteBatch writes.
        let flush = sealer.flush_group();
        let stats = flush.stats();
        assert_eq!(stats.fsyncs_total.get(), 16);
        assert!(
            stats.metadata_batches_total.get() <= 2,
            "16 seals with max_waiters=8 must write metadata in ≤ 2 batches, got {}",
            stats.metadata_batches_total.get()
        );
        let flush_thread = LAST_FLUSH_THREAD.load(std::sync::atomic::Ordering::Relaxed);
        assert_ne!(
            flush_thread, test_thread,
            "the batch fsync must run on the blocking pool, not a runtime worker"
        );
        // Every segment is folded Sealed in the lifecycle registry (the
        // event log is the only durable segment-state store).
        assert_eq!(lifecycle.registry().len(), 16);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn direct_mode_seal_fsyncs_via_flush_coordinator() {
        // F5: the O_DIRECT arm previously never synced at all
        // (`File::flush()` is a no-op). With the write/flush split the
        // Direct-mode temp file is registered with the flush
        // coordinator, so the fsync counter must tick for it too.
        use oceanfs_core::{SegmentId, SizeTier};

        let dir = tempfile::tempdir().unwrap();
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::new(&LifecycleConfig::default()).with_event_wal(Arc::new(
                crate::segment::event_wal::EventWal::open(
                    dir.path().join("event-wal"),
                    &oceanfs_core::EventWalConfig {
                        event_wal_dir: dir.path().join("event-wal"),
                        event_wal_file_size_bytes: 1024 * 1024,
                        event_wal_fsync_batch_timeout_ms: 10,
                        event_wal_checkpoint_bytes: 1024 * 1024,
                    },
                )
                .await
                .unwrap(),
            )),
        );
        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let config = SealConfig {
            target_size_bytes: 4096,
            seal_timeout_ms: 1000,
            data_dir: dir.path().join("segments"),
            io_mode: IoReadMode::Direct,
            write_mode: SegmentWriteMode::Rename,
            fsync_batch_timeout_ms: 100,
            fsync_max_waiters: 8,
        };
        let sealer = Arc::new(SegmentSealer::new(config, wal, Arc::clone(&lifecycle)));

        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
        let data = Bytes::from(vec![0x7Bu8; 2048]);
        let entries =
            vec![SegmentIndexEntry { offset: 0, length: 2048, blob_key_hash: [0x22; 32] }];
        sealer
            .seal_from_data(
                id,
                SizeTier::Standard,
                data,
                &entries,
                0,
                0,
                0,
                None,
                Some(HashOutput::from_bytes([0xAB; 32])),
            )
            .await
            .expect("direct-mode seal must succeed");

        // The file must exist at its final name AND its sync must have
        // been issued by the flush coordinator (F5).
        assert!(dir.path().join("segments").join(format!("{id}.dat")).exists());
        assert!(
            sealer.flush_group().stats().fsyncs_total.get() >= 1,
            "Direct-mode seal must fsync via the flush coordinator"
        );
    }

    #[tokio::test]
    async fn seal_from_data_with_parity_writes_v2_section() {
        use bytes::Bytes;
        use oceanfs_core::{SegmentId, SizeTier};

        use crate::segment::{
            header::SegmentHeader,
            parity_section::{verify_section_hashes, ParitySection},
        };

        let (sealer, lifecycle, _seg, _entries, dir) = setup().await;
        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
        let data = Bytes::from(vec![0x42u8; 512]);

        // k=4, m=2, strip=64 → 2 full stripes → 4 parity shards,
        // computed by the sealer itself at seal time.
        const K: u8 = 4;
        const M: u8 = 2;

        let _handle = sealer
            .seal_from_data(
                id,
                SizeTier::Standard,
                data.clone(),
                &[],
                K,
                M,
                64,
                None,
                Some(HashOutput::from_bytes([0xAB; 32])),
            )
            .await
            .unwrap();

        let path = dir.path().join("segments").join(format!("{id}.dat"));
        let file = std::fs::read(&path).unwrap();
        let hdr = SegmentHeader::from_bytes(&file).expect("valid header");
        assert!(hdr.parity_offset > 0, "v2 file must record the parity section");
        assert_eq!(hdr.version, crate::segment::header::SEGMENT_VERSION);
        let section_end = (hdr.parity_offset + hdr.parity_size) as usize;
        let section = ParitySection::parse(&file[hdr.parity_offset as usize..section_end])
            .expect("valid section");
        assert_eq!(section.stripe_count, 2);
        assert_eq!(section.k, 4);
        assert_eq!(section.m, 2);
        assert!(verify_section_hashes(&section, &data), "shard hash table must verify");

        // Oracle: every section shard must equal a fresh encode of its
        // stripe's data shards. This pins the SoA→AoS shard ORDER — a
        // permutation (e.g. swapped loop nesting) is otherwise
        // self-consistent with the hash table and silently breaks repair.
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Encoder};
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        });
        for stripe in 0..2 {
            let stripe_shards: Vec<&[u8]> =
                (0..4).map(|d| &data[stripe * 256 + d * 64..stripe * 256 + (d + 1) * 64]).collect();
            let fresh = codec.encode(&stripe_shards, 2).unwrap();
            for p in 0..2 {
                assert_eq!(
                    section.parity_shard(stripe, p),
                    &fresh[p][..],
                    "parity shard (stripe {stripe}, parity {p}) must match a fresh encode"
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn seal_time_encode_runs_on_the_blocking_pool_not_the_runtime_worker() {
        // Pins the single-scheduler boundary: the CPU-bound parity encode
        // must run on tokio's blocking pool (via spawn_blocking), never
        // inline on a runtime worker. A regression that removes the
        // wrapper makes the encode run on this test's runtime thread and
        // the seam records that thread's id.
        use std::hash::{Hash, Hasher};

        use oceanfs_core::SegmentId;

        use crate::segment::parity_section::LAST_ENCODE_THREAD;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        let test_thread = hasher.finish();

        let (sealer, lifecycle, _seg, _entries, _dir) = setup().await;
        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
        // 256 KiB → exactly one complete stripe (k=4, strip=64 KiB).
        let data = bytes::Bytes::from(vec![0x77u8; 256 * 1024]);

        let _handle = sealer
            .seal_from_data(
                id,
                SizeTier::Standard,
                data,
                &[],
                4,
                2,
                65536,
                None,
                Some(HashOutput::from_bytes([0xAB; 32])),
            )
            .await
            .unwrap();

        let encode_thread = LAST_ENCODE_THREAD.load(std::sync::atomic::Ordering::Relaxed);
        assert_ne!(
            encode_thread, test_thread,
            "the parity encode must run on the blocking pool, not the runtime worker"
        );
    }
}
