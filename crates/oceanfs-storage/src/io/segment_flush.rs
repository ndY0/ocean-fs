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
    path::PathBuf,
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
    /// Target directory the file is finalized into — the selected pool
    /// root (ADR-0029 f5) or the legacy segments dir (pool_id 0).
    dir: PathBuf,
    /// The pool-aware observed [`DiskIo`](crate::io::DiskIo) this seal
    /// was written through (g1): the per-file fsync barrier records its
    /// latency/errors on the pool's observer (ADR-0029 §D3 — the EIO-on-
    /// fsync Dead-confirming signal).
    io: Arc<dyn crate::io::DiskIo>,
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
                // seal-time EC encode). The flush is deliberately NOT
                // awaited: awaiting it serializes every batch behind the
                // seal step's event-log group commit (~50 ms per batch),
                // capping the seal rate below the fill rate under
                // sustained load — the unsealed set grows and its WAL
                // entries pin the recent files (the wal_not_unbounded
                // regression). Fire-and-forget lets the cores-sized
                // blocking pool parallelize the batches, and the event
                // group's commit latency is then amortized across
                // concurrent seals instead of paid once per batch.
                // Backpressure is unchanged: the channel capacity (the
                // caller's submit awaits) plus the seal worker's
                // semaphore.
                let lifecycle = Arc::clone(&lifecycle);
                let stats = Arc::clone(&stats_task);
                tokio::task::spawn_blocking(move || {
                    flush_batch(batch, &lifecycle, &stats);
                });
            }
        });

        Self { tx, stats }
    }

    /// Registers a sealed segment for group-committed durability.
    ///
    /// The caller (the seal task) has written `file`'s data (temp file,
    /// **not yet synced**) and awaits the returned future, which
    /// resolves once the flusher has synced the file, finalized it
    /// under `filename` in `dir`, and submitted `meta` in the batch
    /// metadata write. `dir` is the target pool root (or the legacy
    /// segments dir) — the file's durability lands where the seal
    /// selected it. `io` is the pool-aware observed [`DiskIo`]
    /// (crate::io::DiskIo) the seal was written through — the fsync
    /// barrier records on the pool's observer (g1).
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
        dir: PathBuf,
        io: Arc<dyn crate::io::DiskIo>,
    ) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(FlushRegistration { file, filename, op, meta, dir, io, done: done_tx })
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
        // Scoped to the failure test's filename: the static flag may be
        // observed by a concurrently-running test's batch (shared
        // process), so the seam must only fail the registration it was
        // armed for.
        if FAIL_SYNC.load(Ordering::Relaxed) && reg.filename == "fail.dat" {
            // Hygiene: same cleanup as the real error path.
            let _ =
                std::fs::remove_file(crate::io::atomic_write::temp_path(&reg.dir, &reg.filename));
            let _ = reg.done.send(Err(Error::Io(io::Error::other("test seam: sync failed"))));
            continue;
        }

        let FlushRegistration { file, filename, op, meta, dir, io, done } = reg;
        // g1: the per-file fsync barrier runs through the seal's
        // pool-aware observed DiskIo — EIO-on-fsync (the ADR-0029 §D3
        // Dead-confirming signal) is recorded per pool on the observer.
        let sync_result = io.fsync_handle(&file);
        let finalize_result = sync_result.and_then(|()| {
            let mode = match op {
                FinalizeOp::Link => SegmentWriteMode::Tmpfile,
                FinalizeOp::Rename => SegmentWriteMode::Rename,
            };
            finalize_temp(mode, file, &dir, &filename)
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
                let _ = std::fs::remove_file(crate::io::atomic_write::temp_path(&dir, &filename));
                let _ = done.send(Err(Error::Io(e)));
            }
        }
    }

    // Phase 3: one lifecycle seal batch for all successfully finalized
    // files. The coordinator validates every id against the registry
    // (Reserved-only), commits the accepted metadata (phase 1: one
    // RocksDB batch; phase 2: one SealEvent append per id — the event
    // group's group commit batches the fsyncs — then the folds, then
    // one mirror RocksDB batch), then folds each entry — the
    // single-writer invariant (ADR-0025 Decision 1) is preserved end
    // to end.
    //
    // The seal batch is async (the event appends await the event
    // group's group commit). flush_batch runs on the blocking pool
    // (spawn_blocking); the future is driven on the runtime handle —
    // the blocking thread is not an async context, so block_on is
    // legal here.
    if !metas.is_empty() {
        stats.metadata_batches_total.inc();
        let handle = tokio::runtime::Handle::current();
        let results = handle.block_on(lifecycle.seal_finalized_batch(metas));
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
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::new(&LifecycleConfig::default()).with_event_wal(event_wal),
        );
        (store, lifecycle, dir)
    }

    fn make_meta(segment_id: SegmentId) -> SegmentMetadata {
        SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id,
            ec_k: 0,
            ec_m: 0,
            size_tier: SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(0),
        }
    }

    #[tokio::test]
    async fn group_commit_batches_concurrent_registrations() {
        let (_store, lifecycle, dir) = test_metadata_and_lifecycle().await;
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let group = Arc::new(SegmentFlushGroup::new(lifecycle.clone(), 100, 8));

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
                group
                    .submit(
                        file,
                        filename,
                        FinalizeOp::Rename,
                        meta,
                        seg_dir.clone(),
                        Arc::new(crate::io::ObservedIo {
                            pool_id: 0,
                            backend: Arc::new(crate::io::IoBackend::default()),
                            observer: Arc::new(crate::io::NoopIoObserver),
                        }),
                    )
                    .await
                    .unwrap();
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
        // The lifecycle registry folded every seal (the event log is
        // the only durable segment-state store — ADR-0025 Decision 3).
        assert_eq!(lifecycle.registry().len(), 16);
    }

    #[tokio::test]
    async fn sync_failure_reports_error_and_skips_metadata() {
        let (_store, lifecycle, dir) = test_metadata_and_lifecycle().await;
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let group = Arc::new(SegmentFlushGroup::new(lifecycle.clone(), 100, 8));

        FAIL_SYNC.store(true, Ordering::Relaxed);
        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
        let meta = make_meta(id);
        let tmp = seg_dir.join(".tmp.fail");
        std::fs::write(&tmp, vec![0xCD; 512]).unwrap();
        let file = std::fs::File::open(&tmp).unwrap();
        let result = group
            .submit(
                file,
                "fail.dat".into(),
                FinalizeOp::Rename,
                meta,
                seg_dir.clone(),
                Arc::new(crate::io::ObservedIo {
                    pool_id: 0,
                    backend: Arc::new(crate::io::IoBackend::default()),
                    observer: Arc::new(crate::io::NoopIoObserver),
                }),
            )
            .await;
        FAIL_SYNC.store(false, Ordering::Relaxed);

        assert!(result.is_err(), "sync failure must propagate to the waiter");
        assert!(!seg_dir.join("fail.dat").exists(), "finalize must not run after a failed sync");
        // The reserve entry survives (it predates the seal); no seal
        // event may be durable for the failed file.
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

        let group = Arc::new(SegmentFlushGroup::new(lifecycle.clone(), 100, 8));
        let tmp = seg_dir.join(".tmp.pin.dat");
        std::fs::write(&tmp, vec![0xEE; 512]).unwrap();
        let file = std::fs::File::open(&tmp).unwrap();
        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap();
        group
            .submit(
                file,
                "pin.dat".into(),
                FinalizeOp::Rename,
                make_meta(id),
                seg_dir,
                Arc::new(crate::io::ObservedIo {
                    pool_id: 0,
                    backend: Arc::new(crate::io::IoBackend::default()),
                    observer: Arc::new(crate::io::NoopIoObserver),
                }),
            )
            .await
            .unwrap();

        let flush_thread = LAST_FLUSH_THREAD.load(Ordering::Relaxed);
        assert_ne!(
            flush_thread, test_thread,
            "the batch sync must run on the blocking pool, not the runtime worker"
        );
    }
}
