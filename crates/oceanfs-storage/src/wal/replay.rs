//! WAL replay — recover unsealed segment data on node restart.
//!
//! On startup, before the HTTP server binds, this module replays all
//! WAL entries to rebuild in-memory active segments from any writes
//! that were in-flight when the node crashed.
//!
//! After successful replay, the WAL is truncated to prevent double-replay
//! on the next restart.

use std::collections::BTreeSet;

use bytes::Bytes;
use oceanfs_core::{SegmentId, SegmentSizeConfig, SizeTier, WalConfig};
use tracing::{info, warn};

use crate::{
    error::Result,
    segment::{event_wal::DataWalPos, lifecycle::SegmentLifecycleCoordinator, pool::SegmentPool},
    wal::reader::WalReader,
};

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
/// # async fn example(config: &WalConfig, wal_writer: &WalWriter, lifecycle: &oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator) -> oceanfs_storage::Result<()> {
/// let summary =
///     replay_wal(config, wal_writer, &pool_small, &pool_standard, &size_config, |_| false, lifecycle).await?;
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
/// Byte bound for the replay queue's COMPLETE (filled) groups. The
/// reader drains them through the pools once the bound is hit
/// (backpressure), so the buffered transient stays bounded. Partial
/// groups (below the fill target — their entries only end with the
/// crash) are inherently bounded by the crash window and stay queued
/// until the WAL is consumed.
const REPLAY_QUEUE_BOUND: u64 = 64 * 1024 * 1024;

/// One unsealed segment's WAL entries, grouped for sequential rebuild.
struct QueuedSegment {
    segment_id: SegmentId,
    tier: SizeTier,
    entries: Vec<Bytes>,
    bytes: u64,
    /// True when the stored bytes reached the tier's fill target — the
    /// write path would have sealed it, so its entries are complete.
    complete: bool,
}

/// The fill target for a tier (the write path seals at this size).
fn fill_target(size_config: &SegmentSizeConfig, tier: SizeTier) -> u64 {
    match tier {
        SizeTier::Small => size_config.small_target_size,
        SizeTier::Standard | SizeTier::Multi => size_config.default_target_size,
        _ => size_config.default_target_size,
    }
}

/// Rebuilds one queued segment into its pool and seals it: appends the
/// grouped entries in WAL order (exact offset reconstruction), lets the
/// fill-triggered seal fire for filled segments, and force-seals
/// partial ones so the slot frees — the pool's configured slot count
/// never bounds the replay.
///
/// The rebuilt segment is **reserved** through the lifecycle
/// coordinator before its first replayed entry is appended: the seal
/// path validates `Reserved`-only (ADR-0025 Decision 1), and the WAL
/// cleanup protects registered-but-unsealed entries — a rebuilt
/// segment must never be sealed without its reserve.
async fn replay_queued_segment(
    group: QueuedSegment,
    segment_pool_small: &SegmentPool,
    segment_pool_standard: &SegmentPool,
    lifecycle: &SegmentLifecycleCoordinator,
) -> Result<()> {
    let (pool, tier) = match group.tier {
        SizeTier::Small => (segment_pool_small, SizeTier::Small),
        SizeTier::Standard | SizeTier::Multi => (segment_pool_standard, SizeTier::Standard),
        _ => return Ok(()), // inline tier never reaches the pools
    };
    // Reserve BEFORE the first replayed DataEntry: a fill-triggered
    // seal can fire during `append_replayed` below, and the seal's
    // Reserved-only validation must pass (the reserve is also the
    // durable registration the WAL cleanup relies on).
    let (ec_k, ec_m, _strip) = pool.ec_params();
    lifecycle.request_reserve(group.segment_id, tier, ec_k, ec_m).await.map_err(|e| {
        crate::error::Error::Io(std::io::Error::other(format!(
            "failed to reserve rebuilt segment {} during WAL replay: {e}",
            group.segment_id
        )))
    })?;
    for data in &group.entries {
        pool.append_replayed(group.segment_id, data).await?;
    }
    // If the segment did not fill, seal it now to free the slot. Filled
    // segments already left the slot via the fill-triggered seal — this
    // is a no-op for them.
    pool.seal_replayed_partial(group.segment_id).await?;
    let _ = tier;
    Ok(())
}

/// Replays all WAL entries into active segments and truncates the WAL afterward.
///
/// This function is called during node startup before the HTTP server
/// binds. It reads every WAL file, deserializes each entry, groups the
/// unsealed entries by segment, rebuilds each segment sequentially
/// (sealing it on completion — the pool's configured slot count never
/// bounds recovery), tracks the maximum HLC timestamp, and truncates
/// the WAL to prevent double-replay on a subsequent restart.
///
/// Every rebuilt segment is reserved through `lifecycle` before its
/// first replayed entry is appended (the seal path validates
/// `Reserved`-only — ADR-0025 phase 1).
///
/// Inline-tier entries (≤4 KB) are skipped during replay — they are
/// stored directly in RocksDB metadata, not in active segments.
///
/// # Errors
///
/// Returns an error if the WAL directory cannot be read, a segment pool
/// append fails, the rebuilt segment's reserve fails, or truncation
/// fails.
pub async fn replay_wal(
    config: &WalConfig,
    wal_writer: &super::WalWriter,
    segment_pool_small: &SegmentPool,
    segment_pool_standard: &SegmentPool,
    size_config: &SegmentSizeConfig,
    already_sealed: impl Fn(SegmentId) -> bool,
    lifecycle: &SegmentLifecycleCoordinator,
) -> Result<ReplaySummary> {
    let reader = WalReader::open(config)?;

    let mut entries_replayed: usize = 0;
    let mut bytes_replayed: u64 = 0;
    let mut entries_skipped_sealed: usize = 0;
    let mut segments_seen: BTreeSet<SegmentId> = BTreeSet::new();
    let mut max_hlc_wall_time: u64 = 0;
    let mut max_hlc_logical: u32 = 0;

    // The replay queue: unsealed entries grouped by segment, rebuilt
    // sequentially with one pool slot. The pool's configured slot count
    // is a WRITE-PATH tuning knob and must never bound recovery — the
    // WAL can describe more distinct unsealed segments than slots
    // (seal-transit recycling, compression, crash timing), and the old
    // "same slot count" assumption failed startup under load.
    let mut queue: Vec<QueuedSegment> = Vec::new();
    let mut index: std::collections::HashMap<SegmentId, usize> = std::collections::HashMap::new();
    let mut queued_complete_bytes: u64 = 0;

    for entry_result in reader.replay() {
        let entry = entry_result?;

        // Skip entries whose segment was already sealed: the sealed
        // segment's file is durable and its metadata is committed, so
        // the disk copy is authoritative. Rebuilding it here would
        // shadow the disk with a potentially PARTIAL pool copy — the
        // WAL keeps sealed segments' entries until file rotation, and
        // entries spanning a rotation boundary are truncated, so the
        // remaining tail would reconstruct the segment at wrong
        // offsets and corrupt every read of it.
        if already_sealed(entry.segment_id()) {
            entries_skipped_sealed += 1;
            continue;
        }

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

        // Route by the entry's POOL TIER when available: the write path
        // records the destination pool (0 = small, 1 = standard)
        // because size classification is ambiguous for compressed
        // chunks — a 4 MiB logical chunk is a Small-tier object OR a
        // Multi-tier piece, and the latter lives in the STANDARD pool.
        let tier = match entry.tier {
            0 => SizeTier::Small,
            1 => SizeTier::Standard,
            other => {
                warn!(
                    tier = other,
                    "unknown WAL entry tier during replay; using size classification"
                );
                size_config.classify(entry.length as u64)
            }
        };
        if tier == SizeTier::Inline {
            // Inline blobs are stored directly in RocksDB metadata and
            // do not go through the segment pipeline.
            continue;
        }

        // Append the entry to its segment's group.
        let id = entry.segment_id();
        let idx = match index.get(&id) {
            Some(&i) => i,
            None => {
                index.insert(id, queue.len());
                queue.push(QueuedSegment {
                    segment_id: id,
                    tier,
                    entries: Vec::new(),
                    bytes: 0,
                    complete: false,
                });
                queue.len() - 1
            }
        };
        let group = &mut queue[idx];
        group.entries.push(entry.data);
        group.bytes += entry.length as u64;
        if !group.complete && group.bytes >= fill_target(size_config, group.tier) {
            // The segment filled — the write path would have sealed it,
            // so no more entries for it exist in the WAL. Sealable.
            group.complete = true;
            queued_complete_bytes += group.bytes;
        }

        // Backpressure: once the buffered COMPLETE groups exceed the
        // bound, drain them through the pools (one slot at a time) and
        // resume reading. Partial groups stay queued — they are the
        // crash window's residue and are inherently small.
        if queued_complete_bytes >= REPLAY_QUEUE_BOUND {
            let mut remaining = Vec::with_capacity(queue.len());
            for group in queue.drain(..) {
                if group.complete {
                    replay_queued_segment(
                        group,
                        segment_pool_small,
                        segment_pool_standard,
                        lifecycle,
                    )
                    .await?;
                } else {
                    remaining.push(group);
                }
            }
            queue = remaining;
            index.clear();
            for (i, group) in queue.iter().enumerate() {
                index.insert(group.segment_id, i);
            }
            queued_complete_bytes = 0;
        }
    }

    // Finalize: rebuild + seal every queued segment (complete and
    // partial), sequentially — one slot, then the WAL is fully
    // consumed and every rebuilt segment is durable.
    for group in queue.drain(..) {
        replay_queued_segment(group, segment_pool_small, segment_pool_standard, lifecycle).await?;
    }

    tracing::info!(
        entries_replayed,
        entries_skipped_sealed,
        distinct_segments = segments_seen.len(),
        "WAL replay scan complete"
    );
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
///
/// `keep` is the number of most recent files to retain (including the
/// current one). When `is_entry_garbage` is provided, retention is
/// **machine-aware** in addition: a file outside the window is still
/// kept when it contains live entries (ADR-0024 §Retention — an entry
/// at position `p` of segment `S` is garbage iff `S` is Sealed with
/// `data_wal_pos ≥ p`, or Deleted). No metadata-store lookup happens
/// here — liveness is the caller's closure (the lifecycle registry +
/// event positions; the CF-derived `durable_or_deleted` scan is gone).
pub async fn cleanup_old_wal_files(
    config: &WalConfig,
    keep: usize,
    is_entry_garbage: Option<&(dyn Fn(oceanfs_core::SegmentId, DataWalPos) -> bool + Send + Sync)>,
) {
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
    // Liveness is decided by the machine (ADR-0024 §Retention — phase 2):
    // an entry at position `p` of segment `S` is garbage iff `S`'s
    // `SealEvent.data_wal_pos ≥ p` (or `S` is `Deleted`). The closure is
    // backed by the folded lifecycle registry + event positions; the
    // CF-derived `durable_or_deleted` scan is gone. Without a closure
    // (minimal embeddings, tests) the plain retention window applies.
    let retention_floor = current_seq.saturating_sub(keep.saturating_sub(1) as u64);
    let mut removed: usize = 0;
    let mut protected: usize = 0;
    for (seq, path) in &file_paths {
        if *seq < retention_floor {
            // Protect files holding live entries (their only durable
            // copy); sweepable entries are not protected.
            if let Some(is_entry_garbage) = is_entry_garbage {
                if file_contains_live_entries(path, is_entry_garbage) {
                    protected += 1;
                    continue;
                }
            }
            match tokio::fs::remove_file(path).await {
                Ok(()) => removed += 1,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to remove old WAL file")
                }
            }
        }
    }

    if removed > 0 || protected > 0 {
        info!(
            removed,
            protected,
            kept = current_seq,
            "cleaned up old WAL files (protected files hold not-yet-finalized entries)"
        );
    }
}

/// Returns `true` when the WAL file contains an entry that is still
/// **live** per the machine's position rule: an entry at position `p`
/// of segment `S` is garbage iff `S` is `Sealed` with
/// `data_wal_pos ≥ p`, or `S` is `Deleted` (ADR-0024 §Retention). Any
/// entry the closure does NOT classify as garbage is live — the file is
/// the only durable copy of that data and must be protected.
///
/// Scans the file's entries — this runs only for files that would
/// otherwise be deleted (one per rotation), so the read cost is bounded
/// by the rotation window.
fn file_contains_live_entries(
    path: &std::path::Path,
    is_entry_garbage: &(dyn Fn(oceanfs_core::SegmentId, DataWalPos) -> bool + Send + Sync),
) -> bool {
    for entry in super::reader::WalReader::entries_in_file_positions(path.to_path_buf()).flatten() {
        let (pos, wal_entry) = entry;
        if !is_entry_garbage(wal_entry.segment_id(), pos) {
            return true;
        }
    }
    false
}

/// Counts the number of WAL files present in the configured directory.
///
/// Files are named `wal_{seq:08}.log`. Rotation appends new files while
/// [`cleanup_old_wal_files`] prunes replayed ones, so a bounded count is
/// the expected steady state. A count that grows without bound signals
/// that sealing/replay stopped consuming the WAL — this is what the
/// Phase 2 `wal_not_unbounded` assertion monitors.
pub fn count_wal_files(config: &WalConfig) -> usize {
    let dir_path = &config.data_dir;
    let Ok(dir) = std::fs::read_dir(dir_path) else {
        return 0;
    };
    dir.flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("wal_") && name.ends_with(".log")
        })
        .count()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use oceanfs_core::{
        HashOutput, LifecycleConfig, PoolConfig, SegmentMetadata, SegmentSizeConfig, SizeTier,
    };

    use super::*;
    use crate::{
        buffer_pool::BufferPool,
        segment::{
            lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
            pool::SegmentPool,
        },
        wal::{WalEntry, WalWriter},
    };

    /// Creates a lifecycle coordinator over a fresh metadata store (the
    /// replay reserves every rebuilt segment through it).
    async fn make_lifecycle(
        registry: Arc<SegmentLifecycleRegistry>,
    ) -> Arc<SegmentLifecycleCoordinator> {
        let dir = tempfile::tempdir().unwrap();
        let event_wal = Arc::new(
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
        );
        Arc::new(SegmentLifecycleCoordinator::with_registry(registry).with_event_wal(event_wal))
    }

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
        registry: Arc<SegmentLifecycleRegistry>,
    ) -> (SegmentPool, SegmentPool) {
        let pool_cfg = PoolConfig::default();
        let small = SegmentPool::new(
            pool_cfg.clone(),
            SizeTier::Small,
            size_config,
            buffer_pool.clone(),
            None,
            None,
            Arc::clone(&registry),
        )
        .unwrap();
        let standard = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            size_config,
            buffer_pool.clone(),
            None,
            None,
            registry,
        )
        .unwrap();
        (small, standard)
    }

    fn make_entry(segment_id: SegmentId, offset: u64, length: u32) -> WalEntry {
        WalEntry::new(
            segment_id,
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

    /// Builds the machine-backed liveness closure (ADR-0024 §Retention
    /// final form): an entry at `pos` of segment `S` is garbage iff the
    /// registry entry is Sealed with `data_wal_pos ≥ pos`, or Deleted;
    /// an absent entry (phantom or deleted-and-evicted) is unreachable
    /// garbage — sweepable.
    fn machine_liveness(
        registry: &Arc<SegmentLifecycleRegistry>,
    ) -> impl Fn(oceanfs_core::SegmentId, DataWalPos) -> bool + Send + Sync + 'static {
        let registry = Arc::clone(registry);
        move |id, pos| {
            registry
                .get(id)
                .map(|entry| crate::segment::lifecycle::entry_is_garbage(&entry, &pos))
                .unwrap_or(true)
        }
    }

    /// Sealed-metadata helper for the retention tests.
    fn sealed_meta(id: SegmentId) -> SegmentMetadata {
        SegmentMetadata {
            segment_id: id,
            ec_k: 1,
            ec_m: 0,
            size_tier: oceanfs_core::SizeTier::Small,
            merkle_root: Some(HashOutput::from_bytes([0u8; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1_000_000_000_000),
        }
    }

    /// Unsealed-metadata helper for the retention tests.
    fn unsealed_meta(id: SegmentId) -> SegmentMetadata {
        SegmentMetadata { merkle_root: None, sealed_at: None, ..sealed_meta(id) }
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
            length,
            0,
            wall,
            logical,
            HashOutput::from_bytes([0u8; 32]),
            vec![0u8; length as usize].into(),
        )
    }

    #[tokio::test]
    async fn count_wal_files_counts_rotated_files_only() {
        let (wal_config, .., dir) = make_test_env().await;
        assert_eq!(count_wal_files(&wal_config), 0, "empty dir must count zero");

        // The WAL lives in `{temp}/wal` (per make_test_env); rotation
        // produces `wal_{seq:08}.log` files at seq 1, 2, 3.
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();
        for seq in 1..=3u64 {
            let path = wal_dir.join(format!("wal_{seq:08}.log"));
            tokio::fs::write(path, b"entry").await.unwrap();
        }
        // Unrelated files (RocksDB, ports.toml) must not be counted.
        tokio::fs::write(wal_dir.join("rocksdb.log"), b"x").await.unwrap();
        tokio::fs::write(wal_dir.join("ports.toml"), b"x").await.unwrap();

        assert_eq!(count_wal_files(&wal_config), 3, "only wal_*.log files count");
    }

    #[tokio::test]
    async fn cleanup_old_wal_files_keeps_retention_window() {
        // Regression: rotation must retain the most recent files — their
        // entries may belong to still-unsealed segments, the only durable
        // copy of that data. Deleting them loses the data on crash.
        let (wal_config, .., dir) = make_test_env().await;
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();
        for seq in 1..=6u64 {
            let path = wal_dir.join(format!("wal_{seq:08}.log"));
            tokio::fs::write(path, b"entry").await.unwrap();
        }

        // Keep the last 4 files (seq 3..=6); seq 1-2 are deleted.
        cleanup_old_wal_files(&wal_config, 4, None).await;
        assert_eq!(count_wal_files(&wal_config), 4, "retention window must survive");

        // keep=1 retains only the current file.
        for seq in 7..=8u64 {
            let path = wal_dir.join(format!("wal_{seq:08}.log"));
            tokio::fs::write(path, b"entry").await.unwrap();
        }
        cleanup_old_wal_files(&wal_config, 1, None).await;
        assert_eq!(count_wal_files(&wal_config), 1, "keep=1 must retain only the current file");
    }

    #[tokio::test]
    async fn cleanup_protects_files_with_unsealed_entries() {
        // Regression: a file outside the retention window must survive
        // when it holds entries for a segment that is not yet sealed —
        // the WAL is the only durable copy of that data. Rotation used
        // to sweep it, losing the segment on crash.
        let (wal_config, .., dir) = make_test_env().await;
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();

        // A segment written into the OLDEST file (seq 1), still unsealed.
        let unsealed_id = SegmentId::new();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let lifecycle = make_lifecycle(Arc::clone(&registry)).await;
        // Register the segment as UNSEALED (the write path's phantom
        // registration) BEFORE any rotation, exactly as production does.
        registry.reserve(unsealed_id, unsealed_meta(unsealed_id)).unwrap();
        let unsealed_pos;
        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            // Retention liveness: the machine-backed closure.
            writer.set_liveness(Arc::new(machine_liveness(&registry)));
            unsealed_pos = writer.append(make_entry(unsealed_id, 0, 8192)).await.unwrap();
            // Rotate 6 times so the entry lands in a file far outside
            // the retention window (each rotate opens the next seq).
            // The filler entries' segments are SEALED (with their entry
            // positions recorded), so their files remain sweepable —
            // only the unsealed segment's file is protected.
            for _i in 0..6 {
                let filler_id = SegmentId::new();
                registry.reserve(filler_id, unsealed_meta(filler_id)).unwrap();
                let pos = writer.append(make_entry(filler_id, 0, 8192)).await.unwrap();
                lifecycle.record_data_wal_pos(filler_id, pos);
                registry.seal(filler_id, sealed_meta(filler_id)).unwrap();
                writer.rotate().await.unwrap();
            }
        }
        // The seal-aware cleanup protects the oldest file: rotations
        // swept only the intermediate files (their entries' segments are
        // sealed), while file 0 — holding the unsealed segment's entry —
        // survived far beyond the 4-file window.
        let after_rotation = count_wal_files(&wal_config);
        assert_eq!(
            after_rotation, 5,
            "4-file window (seq 3..=6) + protected oldest file (seq 0) = 5"
        );

        cleanup_old_wal_files(&wal_config, 1, Some(&machine_liveness(&registry))).await;
        // The oldest file (holding the unsealed segment's entry) is
        // protected; the current file survives via the window.
        assert_eq!(
            count_wal_files(&wal_config),
            2,
            "oldest file with unsealed entries + current file must survive"
        );

        // Once the segment is SEALED (with its entry position recorded),
        // the file becomes deletable.
        let _ = lifecycle; // the coordinator's record_data_wal_pos path is exercised above
        registry.record_data_wal_pos(unsealed_id, unsealed_pos);
        registry.seal(unsealed_id, sealed_meta(unsealed_id)).unwrap();
        cleanup_old_wal_files(&wal_config, 1, Some(&machine_liveness(&registry))).await;
        assert_eq!(count_wal_files(&wal_config), 1, "sealed segments' entries may be swept");
    }

    #[tokio::test]
    async fn cleanup_sweeps_files_with_only_deleted_entries() {
        // A file outside the window whose entries all belong to DELETED
        // segments must be swept — the deleted-segment marker tells the
        // retention logic the data is garbage, not merely unsealed.
        let (wal_config, .., dir) = make_test_env().await;
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();

        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let lifecycle = make_lifecycle(Arc::clone(&registry)).await;
        let deleted_id = SegmentId::new();
        // Sealed first (the DeleteEvent's garbage rule applies to the
        // sealed state), then deleted.
        registry.reserve(deleted_id, unsealed_meta(deleted_id)).unwrap();
        registry.seal(deleted_id, sealed_meta(deleted_id)).unwrap();
        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            // Retention liveness: the machine-backed closure.
            writer.set_liveness(Arc::new(machine_liveness(&registry)));
            writer.append(make_entry(deleted_id, 0, 8192)).await.unwrap();
            // ...then deleted (the fold evicts with grace 0): file 0 now
            // holds only garbage entries.
            registry.delete(deleted_id).unwrap();
            for _i in 0..6 {
                let filler_id = SegmentId::new();
                registry.reserve(filler_id, unsealed_meta(filler_id)).unwrap();
                let pos = writer.append(make_entry(filler_id, 0, 8192)).await.unwrap();
                lifecycle.record_data_wal_pos(filler_id, pos);
                registry.seal(filler_id, sealed_meta(filler_id)).unwrap();
                writer.rotate().await.unwrap();
            }
        }
        // The deleted segment's file was swept with the rest — only the
        // 4-file window survives (no protected file remains).
        assert_eq!(
            count_wal_files(&wal_config),
            4,
            "files holding only deleted segments' entries must be swept"
        );
    }

    #[tokio::test]
    async fn cleanup_sweeps_files_with_unregistered_entries() {
        // Regression (WAL leak on the phase-2 SUT): a file outside the
        // retention window whose entries reference segments with NO CF
        // entry at all used to be protected forever. Two origins:
        //   1. a crash-window PHANTOM — WAL append happened but the
        //      put_segment registration never landed (replay skips it),
        //   2. an UNSALED deletion — delete_segment writes a marker only
        //      for sealed segments, so the CF has neither entry nor
        //      marker.
        // Both are unreachable garbage: sweeping their files loses
        // nothing. Only REGISTERED-but-unsealed segments protect files.
        let (wal_config, .., dir) = make_test_env().await;
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();

        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let lifecycle = make_lifecycle(Arc::clone(&registry)).await;
        let phantom_id = SegmentId::new();
        let unsealed_deleted_id = SegmentId::new();
        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            // Retention liveness: the machine-backed closure.
            writer.set_liveness(Arc::new(machine_liveness(&registry)));
            // Phantom: append WITHOUT registering the segment — the
            // crash-window shape from the production write path.
            writer.append(make_entry(phantom_id, 0, 8192)).await.unwrap();
            // Unsealed-deleted: register, delete while unsealed (the
            // fold evicts; no entry remains to protect anything).
            registry.reserve(unsealed_deleted_id, unsealed_meta(unsealed_deleted_id)).unwrap();
            writer.append(make_entry(unsealed_deleted_id, 0, 8192)).await.unwrap();
            registry.delete(unsealed_deleted_id).unwrap();
            // Rotate 6 times with sealed filler segments so file 0 falls
            // far outside the 4-file window.
            for _i in 0..6 {
                let filler_id = SegmentId::new();
                registry.reserve(filler_id, unsealed_meta(filler_id)).unwrap();
                let pos = writer.append(make_entry(filler_id, 0, 8192)).await.unwrap();
                lifecycle.record_data_wal_pos(filler_id, pos);
                registry.seal(filler_id, sealed_meta(filler_id)).unwrap();
                writer.rotate().await.unwrap();
            }
        }
        // Seal-aware cleanup: file 0 must NOT survive — both unknown ids
        // resolve to no CF entry. Only the 4-file window remains.
        assert_eq!(
            count_wal_files(&wal_config),
            4,
            "files holding only unregistered entries must be swept"
        );
    }

    #[tokio::test]
    async fn replay_wal_empty_directory_returns_zero_summary() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
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
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
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
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
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
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
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
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
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
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
        .await
        .unwrap();
        assert_eq!(summary.max_hlc_wall_time, 5000);
        assert_eq!(summary.max_hlc_logical, 7);
    }

    #[tokio::test]
    async fn replay_wal_skips_entries_for_already_sealed_segments() {
        // Regression: the WAL keeps sealed segments' entries until file
        // rotation. Rebuilding a sealed segment from a possibly-partial
        // WAL tail would shadow the durable disk file with a corrupt
        // pool copy (BadDigest on every read of that segment).
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let sealed_id = SegmentId::new();
        let unsealed_id = SegmentId::new();

        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            // The sealed segment's entries are present in the WAL...
            writer.append(make_entry(sealed_id, 0, 8192)).await.unwrap();
            writer.append(make_entry(sealed_id, 8192, 4096)).await.unwrap();
            // ...and so are the unsealed segment's. (8192 bytes routes
            // to the small tier — inline entries are skipped by design.)
            writer.append(make_entry(unsealed_id, 0, 8192)).await.unwrap();
        }

        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |id| id == sealed_id, // the sealed segment is durable on disk
            &lifecycle,
        )
        .await
        .unwrap();
        assert_eq!(summary.entries_replayed, 1, "only the unsealed entry is replayed");
        assert_eq!(summary.segments_seen, vec![unsealed_id]);
        // The sealed segment must NOT be rebuilt into the pool.
        assert!(pool_small.try_read(sealed_id, 0, 8192).is_none());
        assert!(pool_small.try_read(unsealed_id, 0, 8192).is_some());
    }

    #[tokio::test]
    async fn replay_wal_seals_rebuilt_segments_and_enqueues_seal_work() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let seg_id = SegmentId::new();
        let blob_len: u32 = 5000; // 5 KB → Small tier, not inline

        // Write 8 entries to the WAL (one segment, partial — below the
        // small fill target).
        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            for i in 0..8 {
                let entry = make_entry(seg_id, i as u64 * blob_len as u64, blob_len);
                writer.append(entry).await.unwrap();
            }
        }

        // Replay into pools — the entries land in pool_small.
        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
        .await
        .unwrap();
        assert_eq!(summary.entries_replayed, 8);
        assert_eq!(summary.bytes_replayed, 40000);

        // The rebuilt (partial) segment is SEALED by the replay — the
        // seal work item is enqueued with the full rebuilt data (the
        // WAL-bridged commit completes at startup; the data is durable,
        // not merely resident in a pool slot).
        let mut rx = pool_small.take_seal_rx().expect("seal receiver");
        let work = rx.try_recv().expect("rebuilt segment must be enqueued for sealing");
        assert_eq!(work.segment_id, seg_id, "seal work must carry the rebuilt segment id");
        assert_eq!(work.segment_data.len(), 40000, "seal work carries the full rebuilt data");
        // The segment's data remains readable through the Sealing
        // read window until the seal worker persists it to disk
        // (ADR-0021) — the replay did not discard it.
        assert!(
            pool_small.try_read(seg_id, 0, 5000).is_some(),
            "sealed segment's data must remain readable via the read window"
        );
    }

    #[tokio::test]
    async fn replay_wal_handles_more_distinct_segments_than_slots() {
        // Regression (phase-2 SUT startup failure): a crash can leave
        // MORE distinct unsealed segments than the pool has slots
        // (seal-transit recycling + crash timing). The old replay bound
        // recovery to the pool's WRITE-PATH slot count and failed
        // startup with "no pool slot available". The queued replay must
        // rebuild and seal every segment sequentially, one slot at a
        // time.
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let blob_len: u32 = 5000;
        let mut ids = Vec::new();
        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            // 8 distinct small segments (the pool has 4 slots) with
            // interleaved entries, each below the fill target.
            for _i in 0..8 {
                let id = SegmentId::new();
                ids.push(id);
                for j in 0..3 {
                    let entry = make_entry(id, j as u64 * blob_len as u64, blob_len);
                    writer.append(entry).await.unwrap();
                }
            }
        }

        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let (pool_small, pool_standard) =
            make_pools(&buffer_pool, &size_config, Arc::clone(&registry));
        let lifecycle = make_lifecycle(registry).await;
        let summary = replay_wal(
            &wal_config,
            &wal_writer,
            &pool_small,
            &pool_standard,
            &size_config,
            |_| false,
            &lifecycle,
        )
        .await
        .unwrap();
        assert_eq!(summary.entries_replayed, 24);
        assert_eq!(summary.segments_seen.len(), 8, "all 8 distinct segments must be rebuilt");

        // Every rebuilt segment's seal work is enqueued (the replay
        // sealed each one to free its slot).
        let mut rx = pool_small.take_seal_rx().expect("seal receiver");
        let mut sealed: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        while let Ok(work) = rx.try_recv() {
            sealed.insert(work.segment_id);
        }
        for id in &ids {
            assert!(sealed.contains(id), "segment {id} must be sealed by the replay");
        }
        assert_eq!(sealed.len(), 8, "every distinct segment must be sealed exactly once");
    }
}
