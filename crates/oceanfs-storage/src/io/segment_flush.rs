//! Group commit for sealed-segment fsync + metadata persistence.
//!
//! Mirrors the WAL's group commit (`crate::wal::sync` — internal) for
//! the seal pipeline (perf rule §3.4): concurrent seal tasks register
//! their temp files with the coordinator, a dedicated flusher collects
//! registrations within a short window, then performs one sync barrier
//! round per file (files are synced individually — the win is
//! amortizing the barrier/queue cost across the burst and moving the
//! fsync off the runtime worker threads). The batch's segment metadata
//! is then persisted through the **lifecycle coordinator** — the only
//! writer of segment state — in ONE RocksDB `WriteBatch`
//! (`SegmentLifecycleCoordinator::seal_finalized_batch`; ADR-0025
//! phase 1: the coordinator validates, writes durably, and folds).
//!
//! ## Ordering guarantees
//!
//! A registration's completion signal fires only after:
//!
//! 1. its file's data is synced (`fdatasync` — the file is never made
//!    visible before its data is durable, preserving the O_TMPFILE /
//!    rename atomicity contract), AND
//! 2. the file is finalized under its final name (`linkat` or
//!    `rename`), AND
//! 3. the segment's seal transition has been validated against the
//!    lifecycle registry, its metadata written durably (the batch
//!    write), and the registry folded (validate → durable → fold).
//!
//! This keeps ADR-0021's invariant intact: the seal worker removes the
//! sealing-data entry only after `seal_from_data` returns `Ok`, which
//! now implies durability of both the file and its metadata.

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use oceanfs_core::SegmentMetadata;
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::{Error, Result},
    io::atomic_write::{finalize_temp, SegmentWriteMode},
    segment::lifecycle::SegmentLifecycleCoordinator,
};

/// How a registered temp file is made visible under its final name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalizeOp {
    /// `O_TMPFILE` → `linkat` (unnamed file, atomic visibility).
    Link,
    /// `.tmp.{filename}` → `rename` (portable fallback).
    Rename,
}

/// A single seal awaiting group-committed durability.
struct FlushRegistration {
    /// Open temp file handle (unnamed `O_TMPFILE` or `.tmp.{filename}`).
    file: std::fs::File,
    /// Final file name in the segment data directory.
    filename: String,
    /// How to make the file visible.
    op: FinalizeOp,
    /// Segment metadata persisted by the lifecycle coordinator in the
    /// batch after the file is durable.
    meta: SegmentMetadata,
    /// Completion signal: `Ok` = file synced + finalized + metadata
    /// committed through the lifecycle coordinator.
    done: oneshot::Sender<Result<()>>,
}

/// Test-visible counters for the flush coordinator (DoD instrumentation).
///
/// Registered as metrics by [`crate::segment::sealer::SegmentSealer`]
/// and readable in tests via [`SegmentFlushGroup::stats`].
#[derive(Debug, Clone)]
pub(crate) struct FlushStats {
    /// Number of `fdatasync`/`sync_data` calls issued (one per file).
    pub(crate) fsyncs_total: oceanfs_core::Counter,
    /// Number of flush cycles (batches) executed.
    pub(crate) batches_total: oceanfs_core::Counter,
    /// Number of RocksDB `WriteBatch` writes issued for segment metadata.
    pub(crate) metadata_batches_total: oceanfs_core::Counter,
}

impl Default for FlushStats {
    fn default() -> Self {
        Self {
            fsyncs_total: oceanfs_core::Counter::new(
                "segment_fsyncs_total".into(),
                "Segment file fsync calls issued by the flush coordinator".into(),
                oceanfs_core::LabelSet::empty(),
            ),
            batches_total: oceanfs_core::Counter::new(
                "segment_flush_batches_total".into(),
                "Segment flush coordinator batch cycles".into(),
                oceanfs_core::LabelSet::empty(),
            ),
            metadata_batches_total: oceanfs_core::Counter::new(
                "segment_metadata_batches_total".into(),
                "RocksDB WriteBatch writes for sealed segment metadata".into(),
                oceanfs_core::LabelSet::empty(),
            ),
        }
    }
}

/// Test seam: when set, the next per-file sync fails (error-path tests).
#[cfg(test)]
pub(crate) static FAIL_SYNC: AtomicBool = AtomicBool::new(false);

/// Test seam: records the thread that performed the batch sync, pinning
/// the "fsyncs run on the blocking pool, never on a runtime worker"
/// boundary.
#[cfg(test)]
pub(crate) static LAST_FLUSH_THREAD: AtomicU64 = AtomicU64::new(0);

/// Group-commit coordinator for sealed-segment durability.
///
/// Constructed lazily by [`crate::segment::sealer::SegmentSealer`] on
/// first seal (a tokio runtime must be active — `seal_from_data` is
/// async, so the requirement is satisfied by construction).
pub(crate) struct SegmentFlushGroup {
    tx: mpsc::Sender<FlushRegistration>,
    stats: Arc<FlushStats>,
}

impl SegmentFlushGroup {
    /// Creates a new flush coordinator and spawns the flusher task.
    ///
    /// `batch_timeout_ms` bounds how long the flusher waits for more
    /// registrations after the first one arrives (the group-commit
    /// window). `max_waiters` is an early-flush trigger: when this
    /// many registrations are pending, the batch flushes immediately.
    ///
    /// `lifecycle` is the single writer of segment lifecycle state: the
    /// flusher hands the batch's sealed metadata to
    /// [`SegmentLifecycleCoordinator::seal_finalized_batch`], which
    /// validates, writes it durably (one RocksDB batch), and folds it
    /// into the registry (ADR-0025 phase 1).
    ///
    /// Must be called from within a tokio runtime context.
    pub(crate) fn new(
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        data_dir: PathBuf,
        batch_timeout_ms: u64,
        max_waiters: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<FlushRegistration>(max_waiters.max(16) * 2);
        let stats = Arc::new(FlushStats::default());
        let stats_task = Arc::clone(&stats);

        tokio::spawn(async move {
            let window = Duration::from_millis(batch_timeout_ms);
            loop {
                // Wait for the first registration of a batch.
                let Some(first) = rx.recv().await else { break };

                let mut batch = Vec::with_capacity(max_waiters);
                batch.push(first);

                // Collect further registrations within the window.
                let deadline = Instant::now() + window;
                while batch.len() < max_waiters {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, rx.recv()).await {
                        Ok(Some(reg)) => batch.push(reg),
                        // Channel closed — flush what we have.
                        Ok(None) => break,
                        // Window expired — flush the batch.
                        Err(_) => break,
                    }
                }

                // Run the blocking I/O (sync + finalize + the lifecycle
                // seal batch) on the blocking pool — never on a runtime
                // worker (single-scheduler discipline, same as the
                // seal-time EC encode).
                let lifecycle = Arc::clone(&lifecycle);
                let data_dir = data_dir.clone();
                let stats = Arc::clone(&stats_task);
                let _ = tokio::task::spawn_blocking(move || {
                    flush_batch(batch, &lifecycle, &data_dir, &stats);
                })
                .await;
            }
        });

        Self { tx, stats }
    }

    /// Registers a sealed segment for group-committed durability.
    ///
    /// The caller (the seal task) has written `file`'s data (temp file,
    /// **not yet synced**) and awaits the returned future, which
    /// resolves once the flusher has synced the file, finalized it
    /// under `filename`, and submitted `meta` in the batch metadata
    /// write.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinator is shut down, the sync or
    /// finalize fails, or the batch metadata write fails.
    pub(crate) async fn submit(
        &self,
        file: std::fs::File,
        filename: String,
        op: FinalizeOp,
        meta: SegmentMetadata,
    ) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(FlushRegistration { file, filename, op, meta, done: done_tx })
            .await
            .map_err(|_| Error::Io(io::Error::other("segment flush coordinator is shut down")))?;
        done_rx.await.map_err(|_| {
            Error::Io(io::Error::other("segment flush coordinator dropped the completion signal"))
        })?
    }

    /// Returns the coordinator's test-visible counters.
    pub(crate) fn stats(&self) -> Arc<FlushStats> {
        Arc::clone(&self.stats)
    }
}

/// Syncs + finalizes every registration in the batch, then persists all
/// segment metadata through the lifecycle coordinator in ONE RocksDB
/// `WriteBatch` (validate → durable → fold).
///
/// Runs on the blocking pool (spawn_blocking) — every call here is a
/// blocking syscall or a synchronous RocksDB write.
fn flush_batch(
    batch: Vec<FlushRegistration>,
    lifecycle: &SegmentLifecycleCoordinator,
    data_dir: &Path,
    stats: &FlushStats,
) {
    #[cfg(test)]
    {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        LAST_FLUSH_THREAD.store(hasher.finish(), Ordering::Relaxed);
    }

    stats.batches_total.inc();

    // Phase 1: kick writeback for every file (non-blocking, Linux only)
    // so the barriers in phase 2 overlap across files.
    #[cfg(target_os = "linux")]
    for reg in &batch {
        let len = reg.file.metadata().map(|m| m.len()).unwrap_or(0);
        let _ = crate::wal::sync_file_range_write(&reg.file, 0, len);
    }

    // Phase 2: per-file barrier + finalize. Collect metadata only for
    // files whose sync AND finalize succeeded.
    let mut metas: Vec<SegmentMetadata> = Vec::with_capacity(batch.len());
    let mut ok_waiters: Vec<oneshot::Sender<Result<()>>> = Vec::with_capacity(batch.len());

    for reg in batch {
        stats.fsyncs_total.inc();

        #[cfg(test)]
        if FAIL_SYNC.load(Ordering::Relaxed) {
            // Hygiene: same cleanup as the real error path.
            let _ =
                std::fs::remove_file(crate::io::atomic_write::temp_path(data_dir, &reg.filename));
            let _ = reg.done.send(Err(Error::Io(io::Error::other("test seam: sync failed"))));
            continue;
        }

        let FlushRegistration { file, filename, op, meta, done } = reg;
        let sync_result = file.sync_data();
        let finalize_result = sync_result.and_then(|()| {
            let mode = match op {
                FinalizeOp::Link => SegmentWriteMode::Tmpfile,
                FinalizeOp::Rename => SegmentWriteMode::Rename,
            };
            finalize_temp(mode, file, data_dir, &filename)
        });

        match finalize_result {
            Ok(()) => {
                metas.push(meta);
                ok_waiters.push(done);
            }
            Err(e) => {
                tracing::warn!(
                    filename = %filename,
                    error = %e,
                    "segment flush sync/finalize failed; metadata not persisted"
                );
                // Hygiene: a failed sync/finalize leaves the temp file
                // behind (`.tmp.{filename}` in rename mode, or the
                // unnamed O_TMPFILE which the kernel reclaims on fd
                // close). Remove the named temp so failed seals do not
                // accumulate disk garbage.
                let _ =
                    std::fs::remove_file(crate::io::atomic_write::temp_path(data_dir, &filename));
                let _ = done.send(Err(Error::Io(e)));
            }
        }
    }

    // Phase 3: one lifecycle seal batch for all successfully finalized
    // files. The coordinator validates every id against the registry
    // (Reserved-only), writes the accepted metadata in ONE RocksDB
    // batch, then folds each entry — the single-writer invariant
    // (ADR-0025 Decision 1) is preserved end to end.
    if !metas.is_empty() {
        stats.metadata_batches_total.inc();
        let results = lifecycle.seal_finalized_batch(metas);
        for (done, result) in ok_waiters.into_iter().zip(results) {
            let _ = done.send(result.map_err(|e| Error::Io(io::Error::other(e.to_string()))));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::{LifecycleConfig, MetadataConfig, SegmentId, SizeTier};

    use super::*;
    use crate::{metadata::RocksDbMetadataStore, segment::lifecycle::SegmentLifecycleCoordinator};

    async fn test_metadata_and_lifecycle(
    ) -> (Arc<RocksDbMetadataStore>, Arc<SegmentLifecycleCoordinator>, tempfile::TempDir) {
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
        (store, lifecycle, dir)
    }

    fn make_meta(segment_id: SegmentId) -> SegmentMetadata {
        SegmentMetadata {
            segment_id,
            ec_k: 0,
            ec_m: 0,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(0),
        }
    }

    #[tokio::test]
    async fn group_commit_batches_concurrent_registrations() {
        let (store, lifecycle, dir) = test_metadata_and_lifecycle().await;
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let group = Arc::new(SegmentFlushGroup::new(lifecycle.clone(), seg_dir.clone(), 100, 8));

        // 16 concurrent registrations with max_waiters=8 → at most 2 batches.
        let mut handles = Vec::new();
        for i in 0..16u64 {
            let group = Arc::clone(&group);
            let seg_dir = seg_dir.clone();
            let id = SegmentId::new();
            // Every registered segment must be Reserved in the
            // registry first — the coordinator validates Reserved-only
            // before the durable seal write.
            lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
            let meta = make_meta(id);
            handles.push(tokio::spawn(async move {
                // Write a temp file (the seal task's job before registering).
                let filename = format!("{i}.dat");
                let tmp = seg_dir.join(format!(".tmp.{filename}"));
                std::fs::write(&tmp, vec![0xAB; 1024]).unwrap();
                let file = std::fs::File::open(&tmp).unwrap();
                group.submit(file, filename, FinalizeOp::Rename, meta).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let stats = group.stats();
        assert_eq!(stats.fsyncs_total.get(), 16);
        assert!(
            stats.batches_total.get() <= 2,
            "16 seals with max_waiters=8 must flush in ≤ 2 batches, got {}",
            stats.batches_total.get()
        );
        assert!(
            stats.metadata_batches_total.get() <= 2,
            "metadata must be written in ≤ 2 RocksDB batches, got {}",
            stats.metadata_batches_total.get()
        );

        // Every file must exist under its final name and every metadata
        // entry must be readable.
        for i in 0..16u64 {
            assert!(seg_dir.join(format!("{i}.dat")).exists());
        }
        let ids: Vec<_> = store
            .list_segments()
            .into_iter()
            .filter_map(|r| r.ok().map(|m| m.segment_id))
            .collect();
        assert_eq!(ids.len(), 16, "all 16 segment metadata entries persisted");
        // The lifecycle registry folded every seal.
        assert_eq!(lifecycle.registry().len(), 16);
    }

    #[tokio::test]
    async fn sync_failure_reports_error_and_skips_metadata() {
        let (store, lifecycle, dir) = test_metadata_and_lifecycle().await;
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let group = Arc::new(SegmentFlushGroup::new(lifecycle.clone(), seg_dir.clone(), 100, 8));

        FAIL_SYNC.store(true, Ordering::Relaxed);
        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
        let meta = make_meta(id);
        let tmp = seg_dir.join(".tmp.fail");
        std::fs::write(&tmp, vec![0xCD; 512]).unwrap();
        let file = std::fs::File::open(&tmp).unwrap();
        let result = group.submit(file, "fail.dat".into(), FinalizeOp::Rename, meta).await;
        FAIL_SYNC.store(false, Ordering::Relaxed);

        assert!(result.is_err(), "sync failure must propagate to the waiter");
        assert!(!seg_dir.join("fail.dat").exists(), "finalize must not run after a failed sync");
        // The reserve entry survives (it predates the seal); the SEAL
        // metadata must not have been written.
        let cf = store.get_segment(id).unwrap().expect("reserve entry still present");
        assert!(cf.sealed_at.is_none(), "seal metadata must not be persisted after a failed sync");
        assert_eq!(
            lifecycle.registry().get(id).unwrap().state,
            crate::segment::lifecycle::SegmentState::Reserved,
            "registry fold must not happen for a file that failed sync"
        );
    }

    #[tokio::test]
    async fn fsync_runs_on_the_blocking_pool_not_the_runtime_worker() {
        use std::hash::{Hash, Hasher};

        let (_store, lifecycle, dir) = test_metadata_and_lifecycle().await;
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        let test_thread = hasher.finish();

        let group = Arc::new(SegmentFlushGroup::new(lifecycle.clone(), seg_dir.clone(), 100, 8));
        let tmp = seg_dir.join(".tmp.pin.dat");
        std::fs::write(&tmp, vec![0xEE; 512]).unwrap();
        let file = std::fs::File::open(&tmp).unwrap();
        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
        group.submit(file, "pin.dat".into(), FinalizeOp::Rename, make_meta(id)).await.unwrap();

        let flush_thread = LAST_FLUSH_THREAD.load(Ordering::Relaxed);
        assert_ne!(
            flush_thread, test_thread,
            "the batch sync must run on the blocking pool, not the runtime worker"
        );
    }
}
