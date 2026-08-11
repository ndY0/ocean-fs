//! WAL replay — recover unsealed segment data on node restart.
//!
//! On startup, before the HTTP server binds, this module replays all
//! WAL entries to rebuild in-memory active segments from any writes
//! that were in-flight when the node crashed.
//!
//! After successful replay, the WAL is truncated to prevent double-replay
//! on the next restart.

use std::collections::BTreeSet;

use oceanfs_core::{SegmentId, SegmentSizeConfig, SizeTier, WalConfig};
use tracing::{info, warn};

use crate::{error::Result, segment::pool::SegmentPool, wal::reader::WalReader};

/// Summary of a WAL replay operation.
///
/// Returned by [`replay_wal`] after all WAL entries have been iterated.
/// The `max_hlc_*` fields allow the caller to rebuild the HLC clock to
/// the most recent known timestamp after a crash.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_core::WalConfig;
/// use oceanfs_storage::wal::{replay_wal, WalWriter};
///
/// # async fn example(config: &WalConfig, wal_writer: &WalWriter) -> oceanfs_storage::Result<()> {
/// let summary = replay_wal(config, wal_writer).await?;
/// assert_eq!(summary.entries_replayed, 0); // empty WAL on first start
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ReplaySummary {
    /// Total number of WAL entries replayed.
    pub entries_replayed: usize,

    /// Total bytes of entry data replayed.
    pub bytes_replayed: u64,

    /// Unique segment IDs encountered during replay.
    pub segments_seen: Vec<SegmentId>,

    /// Maximum HLC wall time observed across all replayed entries.
    pub max_hlc_wall_time: u64,

    /// Maximum HLC logical counter observed across all replayed entries.
    pub max_hlc_logical: u32,
}
/// Replays all WAL entries into active segments and truncates the WAL afterward.
///
/// This function is called during node startup before the HTTP server
/// binds. It reads every WAL file, deserializes each entry, appends the
/// inline data into the appropriate tier's [`crate::segment::SegmentPool`], tracks the
/// maximum HLC timestamp, and truncates the WAL to prevent double-replay
/// on a subsequent restart.
///
/// Inline-tier entries (≤4 KB) are skipped during replay — they are
/// stored directly in RocksDB metadata, not in active segments.
///
/// # Errors
///
/// Returns an error if the WAL directory cannot be read, a segment pool
/// append fails, or truncation fails.
pub async fn replay_wal(
    config: &WalConfig,
    wal_writer: &super::WalWriter,
    segment_pool_small: &SegmentPool,
    segment_pool_standard: &SegmentPool,
    size_config: &SegmentSizeConfig,
) -> Result<ReplaySummary> {
    let reader = WalReader::open(config)?;

    let mut entries_replayed: usize = 0;
    let mut bytes_replayed: u64 = 0;
    let mut segments_seen: BTreeSet<SegmentId> = BTreeSet::new();
    let mut max_hlc_wall_time: u64 = 0;
    let mut max_hlc_logical: u32 = 0;

    for entry_result in reader.replay() {
        let entry = entry_result?;
        entries_replayed += 1;
        bytes_replayed += entry.length as u64;
        segments_seen.insert(entry.segment_id());

        // Track the most recent HLC timestamp for clock reconstruction.
        if entry.hlc_wall_time > max_hlc_wall_time
            || (entry.hlc_wall_time == max_hlc_wall_time && entry.hlc_logical > max_hlc_logical)
        {
            max_hlc_wall_time = entry.hlc_wall_time;
            max_hlc_logical = entry.hlc_logical;
        }

        // Reconstruct the active segment: route by blob size tier.
        let tier = size_config.classify(entry.length as u64);
        match tier {
            SizeTier::Small => {
                segment_pool_small.append(&entry.data)?;
            }
            SizeTier::Standard | SizeTier::Multi => {
                segment_pool_standard.append(&entry.data)?;
            }
            SizeTier::Inline => {
                // Inline blobs are stored directly in RocksDB metadata
                // and do not go through the segment pipeline.
            }
            _ => {
                warn!(tier = ?tier, "unexpected tier during WAL replay; skipping entry");
            }
        }
    }

    if entries_replayed > 0 {
        // Truncate the current WAL file to its start — all entries
        // have been replayed and rebuilt into in-memory active segments.
        wal_writer.truncate(0).await?;
        info!(
            entries_replayed,
            bytes_replayed,
            segments = segments_seen.len(),
            max_hlc_wall = max_hlc_wall_time,
            max_hlc_logical = max_hlc_logical,
            "WAL replay complete; active segments rebuilt, WAL truncated"
        );
    } else {
        info!("WAL replay complete; no entries to replay");
    }

    Ok(ReplaySummary {
        entries_replayed,
        bytes_replayed,
        segments_seen: segments_seen.into_iter().collect(),
        max_hlc_wall_time,
        max_hlc_logical,
    })
}

/// Removes old WAL files that have been fully replayed or sealed.
///
/// This is a best-effort cleanup: after replay, files with sequence
/// numbers earlier than the current file are no longer needed and
/// can be safely deleted. Failure to delete a file is logged but
/// does not cause the replay to fail.
pub async fn cleanup_old_wal_files(config: &WalConfig) {
    let dir_path = &config.data_dir;

    // If the WAL directory doesn't exist, there's nothing to clean.
    if !dir_path.exists() {
        return;
    }

    // Find the current WAL file sequence (the highest-numbered file).
    let mut current_seq: u64 = 0;
    let mut file_paths = Vec::with_capacity(16);

    match std::fs::read_dir(dir_path) {
        Ok(dir) => {
            for entry in dir.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("wal_") && name.ends_with(".log") {
                    if let Some(seq_str) =
                        name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log"))
                    {
                        if let Ok(seq) = seq_str.parse::<u64>() {
                            if seq > current_seq {
                                current_seq = seq;
                            }
                            file_paths.push((seq, entry.path()));
                        }
                    }
                }
            }
        }
        Err(e) => {
            warn!(dir = %dir_path.display(), error = %e, "failed to scan WAL directory for cleanup");
            return;
        }
    }

    // Delete all files with sequence numbers less than the current one.
    let mut removed: usize = 0;
    for (seq, path) in &file_paths {
        if *seq < current_seq {
            match tokio::fs::remove_file(path).await {
                Ok(()) => removed += 1,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to remove old WAL file")
                }
            }
        }
    }

    if removed > 0 {
        info!(removed, kept = current_seq, "cleaned up old WAL files");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use oceanfs_core::{HashOutput, PoolConfig, SegmentSizeConfig, SizeTier};

    use super::*;
    use crate::{
        buffer_pool::BufferPool,
        segment::pool::SegmentPool,
        wal::{WalEntry, WalWriter},
    };

    async fn make_test_env() -> (WalConfig, SegmentSizeConfig, Arc<BufferPool>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig {
            data_dir: dir.path().join("wal"),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        };
        let size_config = SegmentSizeConfig::default();
        let buffer_pool = Arc::new(BufferPool::new(65536, 8));
        (wal_config, size_config, buffer_pool, dir)
    }

    fn make_pools(
        buffer_pool: &Arc<BufferPool>,
        size_config: &SegmentSizeConfig,
    ) -> (SegmentPool, SegmentPool) {
        let pool_cfg = PoolConfig::default();
        let small = SegmentPool::new(
            pool_cfg.clone(),
            SizeTier::Small,
            size_config,
            buffer_pool.clone(),
            None,
        )
        .unwrap();
        let standard =
            SegmentPool::new(pool_cfg, SizeTier::Standard, size_config, buffer_pool.clone(), None)
                .unwrap();
        (small, standard)
    }

    fn make_entry(segment_id: SegmentId, offset: u64, length: u32) -> WalEntry {
        WalEntry::new(
            segment_id,
            offset,
            length,
            0,
            0,
            HashOutput::from_bytes([0u8; 32]),
            vec![0u8; length as usize].into(),
        )
    }

    fn make_entry_with_hlc(
        segment_id: SegmentId,
        offset: u64,
        length: u32,
        wall: u64,
        logical: u32,
    ) -> WalEntry {
        WalEntry::new(
            segment_id,
            offset,
            length,
            wall,
            logical,
            HashOutput::from_bytes([0u8; 32]),
            vec![0u8; length as usize].into(),
        )
    }

    #[tokio::test]
    async fn replay_wal_empty_directory_returns_zero_summary() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let summary =
            replay_wal(&wal_config, &wal_writer, &pool_small, &pool_standard, &size_config)
                .await
                .unwrap();
        assert_eq!(summary.entries_replayed, 0);
        assert_eq!(summary.bytes_replayed, 0);
        assert!(summary.segments_seen.is_empty());
    }

    #[tokio::test]
    async fn replay_wal_recovers_and_reconstructs() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let seg_id = SegmentId::new();

        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            for i in 0..5 {
                // 5000-byte blobs → Small tier.
                let entry = make_entry(seg_id, i * 5000, 5000);
                writer.append(entry).await.unwrap();
            }
        }

        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let summary =
            replay_wal(&wal_config, &wal_writer, &pool_small, &pool_standard, &size_config)
                .await
                .unwrap();
        assert_eq!(summary.entries_replayed, 5);
        assert_eq!(summary.bytes_replayed, 25000);
        assert_eq!(summary.segments_seen.len(), 1);
        assert_eq!(summary.segments_seen[0], seg_id);
    }

    #[tokio::test]
    async fn replay_wal_truncates_after_successful_replay() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let seg_id = SegmentId::new();

        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            for i in 0..3 {
                let entry = make_entry(seg_id, i * 64, 64);
                writer.append(entry).await.unwrap();
            }
        }

        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let summary =
            replay_wal(&wal_config, &wal_writer, &pool_small, &pool_standard, &size_config)
                .await
                .unwrap();
        assert_eq!(summary.entries_replayed, 3);

        let reader = WalReader::open(&wal_config).unwrap();
        let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>>>().unwrap();
        assert!(entries.is_empty(), "WAL should be empty after truncation");
    }

    #[tokio::test]
    async fn replay_wal_handles_multiple_segments() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();

        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            writer.append(make_entry(seg_a, 0, 50)).await.unwrap();
            writer.append(make_entry(seg_b, 0, 80)).await.unwrap();
            writer.append(make_entry(seg_a, 50, 50)).await.unwrap();
        }

        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let summary =
            replay_wal(&wal_config, &wal_writer, &pool_small, &pool_standard, &size_config)
                .await
                .unwrap();
        assert_eq!(summary.entries_replayed, 3);
        assert_eq!(summary.segments_seen.len(), 2);
    }

    #[tokio::test]
    async fn replay_wal_tracks_max_hlc() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let seg_id = SegmentId::new();

        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            writer.append(make_entry_with_hlc(seg_id, 0, 10, 1000, 1)).await.unwrap();
            writer.append(make_entry_with_hlc(seg_id, 10, 10, 2000, 0)).await.unwrap();
            writer.append(make_entry_with_hlc(seg_id, 20, 10, 1500, 5)).await.unwrap();
        }

        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let summary =
            replay_wal(&wal_config, &wal_writer, &pool_small, &pool_standard, &size_config)
                .await
                .unwrap();
        assert_eq!(summary.entries_replayed, 3);
        assert_eq!(summary.max_hlc_wall_time, 2000);
        assert_eq!(summary.max_hlc_logical, 0);
    }

    #[tokio::test]
    async fn replay_wal_hlc_tiebreak_by_logical() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let seg_id = SegmentId::new();

        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            writer.append(make_entry_with_hlc(seg_id, 0, 10, 5000, 3)).await.unwrap();
            writer.append(make_entry_with_hlc(seg_id, 10, 10, 5000, 7)).await.unwrap();
        }

        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let summary =
            replay_wal(&wal_config, &wal_writer, &pool_small, &pool_standard, &size_config)
                .await
                .unwrap();
        assert_eq!(summary.max_hlc_wall_time, 5000);
        assert_eq!(summary.max_hlc_logical, 7);
    }

    #[tokio::test]
    async fn replay_wal_reconstructed_data_is_readable_via_pool() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let seg_id = SegmentId::new();
        let blob_len: u32 = 5000; // 5 KB → Small tier, not inline

        // Write 8 entries to the WAL (2× the pool size so round-robin wraps).
        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            for i in 0..8 {
                let entry = make_entry(seg_id, i as u64 * blob_len as u64, blob_len);
                writer.append(entry).await.unwrap();
            }
        }

        // Replay into pools — the entries land in pool_small.
        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let summary =
            replay_wal(&wal_config, &wal_writer, &pool_small, &pool_standard, &size_config)
                .await
                .unwrap();
        assert_eq!(summary.entries_replayed, 8);
        assert_eq!(summary.bytes_replayed, 40000);

        // With 8 entries and 4 pool slots (round-robin), each slot
        // received 2 entries = 10000 bytes. Append one more blob —
        // it lands in the same slot as entry 0/4/8. The offset should
        // be ≥ 10000 (the cumulative size of the first 2 entries in
        // that slot).
        let new_data = vec![0xCCu8; 200];
        let (_seg_id, offset, _len) = pool_small.append(&new_data).unwrap();
        assert!(
            offset >= 10000,
            "after replay of 8×5000 bytes (2 per slot), next append offset {offset} \
             should be ≥ 10000 (replayed data not present in active segment)"
        );
    }
}
