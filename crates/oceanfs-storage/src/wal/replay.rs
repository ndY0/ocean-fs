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
    metadata::RocksDbMetadataStore,
    segment::{lifecycle::SegmentLifecycleCoordinator, pool::SegmentPool},
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
/// current one). When `metadata` is provided, retention is **seal-aware**
/// in addition: a file outside the window is still kept when it contains
/// entries for segments that are registered but not yet sealed
/// (`sealed_at: None`) — the WAL is the only durable copy of an unsealed
/// segment's data, and replay reads every retained file. Segments that
/// completed sealing are durable on disk, so their entries may be swept
/// freely. Entries for segments that are **not registered in the
/// metadata store at all** are unreachable garbage (a phantom whose
/// `put_segment` never landed — crash window — or a segment deleted
/// without a marker); they do not protect their file. The write path
/// registers the phantom BEFORE the WAL entry, so the unregistered case
/// can only be a true crash phantom — no timing heuristic needed.
pub async fn cleanup_old_wal_files(
    config: &WalConfig,
    keep: usize,
    metadata: Option<&RocksDbMetadataStore>,
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
    // The set of segments whose data IS durable outside the WAL (sealed
    // files) plus segments whose data was intentionally DELETED (GC
    // compaction / orphan reaper — their WAL entries are garbage). For
    // any entry referencing a segment in neither set, the cleanup
    // resolves the id against the metadata store: a segment that is
    // REGISTERED but unsealed is still in flight and must protect its
    // file (the WAL is its only durable copy); a segment with no CF
    // entry at all is unreachable garbage — either a phantom whose
    // registration never landed (the write path registers after the WAL
    // append; a crash in between leaves entries with no CF entry) or a
    // segment deleted without a marker (unsealed deletion) — and its
    // entries may be swept.
    let durable_or_deleted: std::collections::HashSet<oceanfs_core::SegmentId> = metadata
        .map(|m| {
            let mut set: std::collections::HashSet<oceanfs_core::SegmentId> = m
                .list_segments()
                .into_iter()
                .filter_map(|r| r.ok())
                .filter(|meta| meta.sealed_at.is_some())
                .map(|meta| meta.segment_id)
                .collect();
            set.extend(
                m.list_deleted_segments().into_iter().filter_map(|r| r.ok()).map(|(id, _)| id),
            );
            set
        })
        .unwrap_or_default();

    // Delete all files outside the retention window — unless they hold
    // entries for segments that are neither durable nor deleted, where
    // "live" is decided by CF resolution (seal-aware mode, active only
    // when a metadata store is provided; without one the plain window
    // applies).
    let seal_aware = metadata.is_some();
    let retention_floor = current_seq.saturating_sub(keep.saturating_sub(1) as u64);
    let mut removed: usize = 0;
    let mut protected: usize = 0;
    for (seq, path) in &file_paths {
        if *seq < retention_floor {
            // Protect files holding entries for registered-but-unsealed
            // segments; entries for unregistered ids are sweepable.
            if seal_aware && file_contains_live_entries(path, &durable_or_deleted, metadata) {
                protected += 1;
                continue;
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

/// Returns `true` when the WAL file contains an entry for a segment
/// that is still **live**: registered in the metadata store but not
/// sealed — the file is the only durable copy of that data.
///
/// Entries for sealed or deleted segments (the `durable_or_deleted`
/// set) and entries for segments with **no CF entry at all** (never
/// registered, or deleted without a marker) are sweepable: sealed data
/// lives on disk, deleted data is garbage, and unregistered ids are
/// unreachable (their writes were never visible).
///
/// Scans the file's entries — this runs only for files that would
/// otherwise be deleted (one per rotation), so the read cost is bounded
/// by the rotation window.
fn file_contains_live_entries(
    path: &std::path::Path,
    durable_or_deleted: &std::collections::HashSet<oceanfs_core::SegmentId>,
    metadata: Option<&RocksDbMetadataStore>,
) -> bool {
    for entry in super::reader::WalReader::entries_in_file(path.to_path_buf()).flatten() {
        let id = entry.segment_id();
        if durable_or_deleted.contains(&id) {
            continue;
        }
        // Not durable and not deleted — decide by CF resolution.
        match metadata {
            Some(m) => match m.get_segment(id) {
                Ok(Some(meta)) if meta.sealed_at.is_none() => {
                    // Registered but unsealed: in-flight segment whose
                    // only durable copy is this file → protect.
                    return true;
                }
                Ok(Some(_)) => {} // sealed (already in the set; harmless double-check)
                Ok(None) => {
                    // No CF entry: either the registration never landed
                    // (crash-window phantom) or the segment was deleted
                    // without a marker (unsealed deletion). Either way
                    // the entries are unreachable — sweepable. (The
                    // write path registers the phantom BEFORE the WAL
                    // entry, so no in-flight segment can look
                    // unregistered.)
                }
                Err(e) => {
                    // DB read failure: we cannot prove the segment is
                    // unreachable — protect the file (conservative).
                    warn!(segment = %id, error = %e, "failed to resolve segment during WAL cleanup; protecting file");
                    return true;
                }
            },
            None => return true, // unreachable (seal-aware callers only); protect
        }
    }
    false
}

/// Prunes deleted-segment markers whose segment is no longer referenced
/// by any retained WAL file.
///
/// Markers accumulate while their segments' entries sit in retained
/// files (the entries only become sweepable once the file leaves the
/// retention window); once no retained file references the segment, the
/// marker is pure garbage. Runs periodically during operation (the
/// writer throttles it to every `MARKER_PRUNE_ROTATIONS` rotations) and
/// once at replay, where the retained-file scan is already paid for.
///
/// Returns the number of markers removed.
pub async fn prune_deleted_segment_markers(
    config: &WalConfig,
    metadata: &RocksDbMetadataStore,
) -> usize {
    // Collect the segment ids referenced by every retained file.
    let mut referenced: std::collections::HashSet<oceanfs_core::SegmentId> =
        std::collections::HashSet::new();
    let Ok(dir) = std::fs::read_dir(&config.data_dir) else {
        return 0;
    };
    for entry in dir.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal_") && name.ends_with(".log") {
            for wal_entry in super::reader::WalReader::entries_in_file(entry.path()).flatten() {
                referenced.insert(wal_entry.segment_id());
            }
        }
    }

    // Drop every marker whose segment is not referenced anymore.
    let mut pruned = 0usize;
    for result in metadata.list_deleted_segments() {
        let Ok((id, _)) = result else { continue };
        if !referenced.contains(&id) && metadata.delete_deleted_segment(id).is_ok() {
            pruned += 1;
        }
    }
    pruned
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
        HashOutput, LifecycleConfig, MetadataConfig, PoolConfig, SegmentMetadata,
        SegmentSizeConfig, SizeTier,
    };

    use super::*;
    use crate::{
        buffer_pool::BufferPool,
        metadata::RocksDbMetadataStore,
        segment::{lifecycle::SegmentLifecycleCoordinator, pool::SegmentPool},
        wal::{WalEntry, WalWriter},
    };

    /// Creates a lifecycle coordinator over a fresh metadata store (the
    /// replay reserves every rebuilt segment through it).
    async fn make_lifecycle() -> (Arc<RocksDbMetadataStore>, Arc<SegmentLifecycleCoordinator>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        let lifecycle =
            Arc::new(SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default()));
        (store, lifecycle)
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
    ) -> (SegmentPool, SegmentPool) {
        let pool_cfg = PoolConfig::default();
        let small = SegmentPool::new(
            pool_cfg.clone(),
            SizeTier::Small,
            size_config,
            buffer_pool.clone(),
            None,
            None,
        )
        .unwrap();
        let standard = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            size_config,
            buffer_pool.clone(),
            None,
            None,
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
        let metadata = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("metadata"),
                ..Default::default()
            })
            .unwrap(),
        );
        // Register the segment as UNSEALED (sealed_at: None — the write
        // path's phantom registration) BEFORE any rotation, exactly as
        // the production write path does.
        metadata
            .put_segment(SegmentMetadata {
                segment_id: unsealed_id,
                ec_k: 1,
                ec_m: 0,
                size_tier: oceanfs_core::SizeTier::Small,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: None,
            })
            .unwrap();
        {
            let writer =
                WalWriter::open(&wal_config).await.unwrap().with_metadata(Arc::clone(&metadata));
            writer.append(make_entry(unsealed_id, 0, 8192)).await.unwrap();
            // Rotate 6 times so the entry lands in a file far outside
            // the retention window (each rotate opens the next seq).
            // The filler entries' segments are SEALED (registered with
            // sealed_at: Some), so their files remain sweepable — only
            // the unsealed segment's file is protected.
            for _i in 0..6 {
                let filler_id = SegmentId::new();
                metadata
                    .put_segment(SegmentMetadata {
                        segment_id: filler_id,
                        ec_k: 1,
                        ec_m: 0,
                        size_tier: oceanfs_core::SizeTier::Small,
                        merkle_root: None,
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: Some(1_000_000_000_000),
                    })
                    .unwrap();
                writer.append(make_entry(filler_id, 0, 8192)).await.unwrap();
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

        cleanup_old_wal_files(&wal_config, 1, Some(&metadata)).await;
        // The oldest file (holding the unsealed segment's entry) is
        // protected; the current file survives via the window.
        assert_eq!(
            count_wal_files(&wal_config),
            2,
            "oldest file with unsealed entries + current file must survive"
        );

        // Once the segment is SEALED, the file becomes deletable.
        metadata
            .put_segment(SegmentMetadata {
                segment_id: unsealed_id,
                ec_k: 1,
                ec_m: 0,
                size_tier: oceanfs_core::SizeTier::Small,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1_000_000_000_000),
            })
            .unwrap();
        cleanup_old_wal_files(&wal_config, 1, Some(&metadata)).await;
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

        let metadata = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("metadata"),
                ..Default::default()
            })
            .unwrap(),
        );
        let deleted_id = SegmentId::new();
        // Sealed first (delete_segment only marks SEALED deletions)...
        metadata
            .put_segment(SegmentMetadata {
                segment_id: deleted_id,
                ec_k: 1,
                ec_m: 0,
                size_tier: oceanfs_core::SizeTier::Small,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1_000_000_000_000),
            })
            .unwrap();
        {
            let writer =
                WalWriter::open(&wal_config).await.unwrap().with_metadata(Arc::clone(&metadata));
            writer.append(make_entry(deleted_id, 0, 8192)).await.unwrap();
            // ...then deleted: file 0 now holds only garbage entries.
            metadata.delete_segment(deleted_id).unwrap();
            for _i in 0..6 {
                let filler_id = SegmentId::new();
                metadata
                    .put_segment(SegmentMetadata {
                        segment_id: filler_id,
                        ec_k: 1,
                        ec_m: 0,
                        size_tier: oceanfs_core::SizeTier::Small,
                        merkle_root: None,
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: Some(1_000_000_000_000),
                    })
                    .unwrap();
                writer.append(make_entry(filler_id, 0, 8192)).await.unwrap();
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

        let metadata = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("metadata"),
                ..Default::default()
            })
            .unwrap(),
        );
        let phantom_id = SegmentId::new();
        let unsealed_deleted_id = SegmentId::new();
        {
            let writer =
                WalWriter::open(&wal_config).await.unwrap().with_metadata(Arc::clone(&metadata));
            // Phantom: append WITHOUT registering the segment — the
            // crash-window shape from the production write path.
            writer.append(make_entry(phantom_id, 0, 8192)).await.unwrap();
            // Unsealed-deleted: register, delete while unsealed (no
            // marker is written — was_sealed is false).
            metadata
                .put_segment(SegmentMetadata {
                    segment_id: unsealed_deleted_id,
                    ec_k: 1,
                    ec_m: 0,
                    size_tier: oceanfs_core::SizeTier::Small,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                })
                .unwrap();
            writer.append(make_entry(unsealed_deleted_id, 0, 8192)).await.unwrap();
            metadata.delete_segment(unsealed_deleted_id).unwrap();
            // Rotate 6 times with sealed filler segments so file 0 falls
            // far outside the 4-file window.
            for _i in 0..6 {
                let filler_id = SegmentId::new();
                metadata
                    .put_segment(SegmentMetadata {
                        segment_id: filler_id,
                        ec_k: 1,
                        ec_m: 0,
                        size_tier: oceanfs_core::SizeTier::Small,
                        merkle_root: None,
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: Some(1_000_000_000_000),
                    })
                    .unwrap();
                writer.append(make_entry(filler_id, 0, 8192)).await.unwrap();
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
    async fn prune_deleted_segment_markers_removes_only_unreferenced() {
        let (wal_config, .., dir) = make_test_env().await;
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();

        let metadata = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("metadata"),
                ..Default::default()
            })
            .unwrap(),
        );
        let referenced_id = SegmentId::new();
        let unreferenced_id = SegmentId::new();

        // A retained WAL file referencing `referenced_id`.
        {
            let writer = WalWriter::open(&wal_config).await.unwrap();
            writer.append(make_entry(referenced_id, 0, 8192)).await.unwrap();
        }
        // Both segments are marked deleted; only `referenced_id` still
        // has entries in a retained file.
        metadata.put_deleted_segment(referenced_id, 100).unwrap();
        metadata.put_deleted_segment(unreferenced_id, 200).unwrap();

        let pruned = prune_deleted_segment_markers(&wal_config, &metadata).await;
        assert_eq!(pruned, 1, "only the unreferenced marker is pruned");
        let remaining: Vec<_> =
            metadata.list_deleted_segments().into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(remaining, vec![(referenced_id, 100)]);
    }

    #[tokio::test]
    async fn replay_wal_empty_directory_returns_zero_summary() {
        let (wal_config, size_config, buffer_pool, _dir) = make_test_env().await;
        let wal_writer = WalWriter::open(&wal_config).await.unwrap();
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
        let (pool_small, pool_standard) = make_pools(&buffer_pool, &size_config);
        let (_store, lifecycle) = make_lifecycle().await;
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
