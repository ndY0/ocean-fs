//! Crash-window fault-injection matrix — rows 1–6 of ADR-0025
//! §Crash-window table (test contract, not documentation).
//!
//! Each row drives the coordinator to its milestone, **kills the
//! process** (drops every instance — the on-disk state is all that
//! survives), reopens from that state, runs
//! [`SegmentLifecycleCoordinator::rebuild_with_data_wal`], and asserts
//! (a) the folded state exactly matches the table's "Folded state"
//! column, (b) the recovery action column was performed, (c) the
//! [`RebuildOutcome`] vector matches.
//!
//! Row 6 ("Sealed, file missing") is **unrepresentable**: the machine's
//! API cannot express unlink-before-delete — the test asserts the API
//! shape (no unlink operation exists; the delete transition requires a
//! live entry) and that recovery never fabricates a `DeleteEvent` for a
//! missing file.

#![cfg(test)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{
    EventWalConfig, HashOutput, LifecycleConfig, PoolConfig, SegmentId, SegmentMetadata,
    SegmentSizeConfig, SizeTier, WalConfig,
};

use crate::{
    buffer_pool::BufferPool,
    segment::{
        event_checkpoint::EventCheckpoint,
        event_wal::{EventWal, EventWalPos, SegmentEvent},
        lifecycle::{
            entry_is_garbage, RebuildOutcome, SegmentLifecycleCoordinator,
            SegmentLifecycleRegistry, SegmentState,
        },
        pool::SegmentPool,
    },
    wal::{WalEntry, WalReader, WalWriter},
    DataWalPos, SealConfig, SegmentSealer,
};

/// The deterministic merkle-root builder shared by the seal worker and
/// the recovery pass: `blake3(data)`. "Matching roots" assertions use
/// this same construction on both sides.
fn root_fn(data: &[u8]) -> Option<HashOutput> {
    Some(HashOutput::from_bytes(*blake3::hash(data).as_bytes()))
}

/// A mini seal worker: drains both pools' seal queues and seals each
/// work item with the test root (the production seal worker's shape —
/// the merkle root is computed at seal time).
fn spawn_seal_worker(
    pool_small: Arc<SegmentPool>,
    pool_standard: Arc<SegmentPool>,
    sealer: Arc<SegmentSealer>,
    root_fn: impl Fn(&[u8]) -> Option<HashOutput> + Copy + Send + Sync + 'static,
) -> tokio::task::JoinHandle<()> {
    let rx_small = pool_small.take_seal_rx().expect("small pool seal rx");
    let rx_standard = pool_standard.take_seal_rx().expect("standard pool seal rx");
    tokio::spawn(async move {
        let mut rx_small = rx_small;
        let mut rx_standard = rx_standard;
        loop {
            let work = tokio::select! {
                w = rx_small.recv() => w,
                w = rx_standard.recv() => w,
            };
            let Some(work) = work else { break };
            let sealer = Arc::clone(&sealer);
            tokio::spawn(async move {
                let root = root_fn(&work.segment_data);
                let _ = sealer
                    .seal_from_data(
                        work.segment_id,
                        work.tier,
                        work.segment_data,
                        &[],
                        work.ec_k,
                        work.ec_m,
                        work.strip_size_bytes,
                        work.ec_encoder,
                        root,
                    )
                    .await;
            });
        }
    })
}

/// The crash-matrix harness: a fully wired node slice — store, event
/// wal, data wal, pools, coordinator (event-wal arm + idle-seal pools),
/// sealer, and the mini seal worker.
struct Harness {
    event_wal: Arc<EventWal>,
    checkpoint: Option<Arc<EventCheckpoint>>,
    data_wal: Arc<WalWriter>,
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    sealer: Arc<SegmentSealer>,
    wal_dir: std::path::PathBuf,
    segments_dir: std::path::PathBuf,
    _worker: tokio::task::JoinHandle<()>,
}

impl Harness {
    /// "Boots" the node slice on `dir` (fresh or reopened) without the
    /// checkpoint trigger.
    async fn boot(dir: &std::path::Path) -> Harness {
        Self::boot_with(dir, None).await
    }

    /// "Boots" the node slice with the checkpoint trigger wired at the
    /// given byte threshold.
    async fn boot_ckpt(dir: &std::path::Path, threshold_bytes: u64) -> Harness {
        Self::boot_with(dir, Some(threshold_bytes)).await
    }

    /// "Boots" the node slice on `dir`: store, event wal (+ optional
    /// checkpoint), data wal, pools, coordinator (event-wal arm +
    /// idle-seal pools), sealer, and the mini seal worker.
    async fn boot_with(dir: &std::path::Path, checkpoint_threshold: Option<u64>) -> Harness {
        let event_wal_config = EventWalConfig {
            event_wal_dir: dir.join("event-wal"),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: checkpoint_threshold.unwrap_or(1024 * 1024),
        };
        let event_wal = Arc::new(
            EventWal::open(event_wal_config.event_wal_dir.clone(), &event_wal_config)
                .await
                .unwrap(),
        );
        let checkpoint = checkpoint_threshold.map(|_| {
            Arc::new(
                EventCheckpoint::open(event_wal_config.event_wal_dir.clone(), event_wal.clone())
                    .unwrap(),
            )
        });
        let data_config = WalConfig {
            data_dir: dir.join("wal"),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            wal_use_sync_file_range: false,
        };
        let data_wal = Arc::new(WalWriter::open(&data_config).await.unwrap());

        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let size_config = SegmentSizeConfig::default();
        let buffer_pool = Arc::new(BufferPool::new(65536, 8));
        let pool_cfg = PoolConfig::default();
        let pool_small = Arc::new(
            SegmentPool::new(
                pool_cfg.clone(),
                SizeTier::Small,
                &size_config,
                buffer_pool.clone(),
                None,
                None,
                Arc::clone(&registry),
            )
            .unwrap(),
        );
        let pool_standard = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_config,
                buffer_pool,
                None,
                None,
                Arc::clone(&registry),
            )
            .unwrap(),
        );
        let mut builder =
            SegmentLifecycleCoordinator::with_registry(registry).with_event_wal(event_wal.clone());
        if let Some(checkpoint) = &checkpoint {
            builder = builder.with_checkpoint(checkpoint.clone(), event_wal_config);
        }
        let lifecycle =
            Arc::new(builder.with_seal_pools(vec![pool_small.clone(), pool_standard.clone()]));
        let sealer = Arc::new(SegmentSealer::new(
            SealConfig { data_dir: dir.join("segments"), ..Default::default() },
            data_wal.clone(),
            lifecycle.clone(),
        ));
        let worker = spawn_seal_worker(pool_small, pool_standard, sealer.clone(), root_fn);

        Harness {
            event_wal,
            checkpoint,
            data_wal,
            lifecycle,
            sealer,
            wal_dir: dir.join("wal"),
            segments_dir: dir.join("segments"),
            _worker: worker,
        }
    }

    /// "Kills the process": the seal worker is aborted and joined (it
    /// holds Arc clones of the store through the sealer), then every
    /// instance is dropped — only the on-disk state survives.
    async fn crash(self) {
        // Abort and JOIN the seal worker: it holds Arc clones of the
        // sealer (→ lifecycle → metadata store), so the RocksDB lock is
        // only released once the worker's captures drop.
        self._worker.abort();
        let _ = self._worker.await;
    }

    /// The recovery action: fold + dual-read + data-WAL pass from the
    /// earliest retained event.
    async fn recover(&self) -> RebuildOutcome {
        self.recover_from(EventWalPos { file_seq: 0, offset: 0 }).await
    }

    /// The recovery action from a given fold start (a checkpoint's
    /// covered position).
    async fn recover_from(&self, start: EventWalPos) -> RebuildOutcome {
        let reader = WalReader::open(&WalConfig {
            data_dir: self.wal_dir.clone(),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            wal_use_sync_file_range: false,
        })
        .unwrap();
        self.lifecycle
            .rebuild_with_data_wal(
                self.event_wal.read_from(start),
                &reader,
                &self.sealer,
                root_fn,
                &self.data_wal,
            )
            .await
            .unwrap()
    }

    /// Waits (bounded) for a condition — the checkpoint task runs
    /// asynchronously off the append path.
    async fn wait_until(&self, mut cond: impl FnMut(&Harness) -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !cond(self) {
            assert!(std::time::Instant::now() < deadline, "condition not met within 5s");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Counts the `checkpoint-*` files on disk (`.tmp` orphans excluded).
    fn checkpoint_file_count(&self) -> usize {
        std::fs::read_dir(self.event_wal.dir())
            .map(|d| {
                d.flatten()
                    .filter(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        name.starts_with("checkpoint-") && !name.ends_with(".tmp")
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    /// Drive helpers — the milestone sequence per row.
    async fn reserve(&self, id: SegmentId) {
        self.lifecycle.request_reserve(id, SizeTier::Small, 4, 2).await.unwrap();
    }

    async fn append(&self, id: SegmentId, offset: u64, data: &[u8]) -> DataWalPos {
        let entry = WalEntry::new(
            id,
            offset,
            data.len() as u32,
            data.len() as u32,
            0, // small-tier pool byte
            0,
            0,
            HashOutput::from_bytes([0u8; 32]),
            Bytes::copy_from_slice(data),
        );
        self.sealer.append_wal_entry(entry).await.unwrap()
    }

    async fn seal(&self, id: SegmentId, data: &[u8]) {
        let meta = SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Small,
            merkle_root: root_fn(data),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1_700_000_000_000),
        };
        self.lifecycle.request_seal(id, meta, None).await.unwrap();
    }

    async fn delete(&self, id: SegmentId) {
        self.lifecycle.request_delete(id).await.unwrap();
    }

    fn entry(&self, id: SegmentId) -> crate::segment::lifecycle::LifecycleEntry {
        self.lifecycle.registry().get(id).expect("registry entry present")
    }
}

// ---------------------------------------------------------------------------
// Row 1: kill between `ReserveEvent` and the first `DataEntry`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row1_kill_before_first_data_entry_drops_empty_reserve() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // Folded state: Reserved, empty → drop the reserve.
    assert_eq!(outcome.folded_segments, 1);
    assert_eq!(outcome.dropped_empty_reserves, 1);
    assert_eq!(outcome.re_sealed_segments, 0);
    assert_eq!(outcome.adopted_segments, 0);
    assert_eq!(outcome.swept_entries, 0);
    assert!(
        h.lifecycle.registry().get(id).is_none(),
        "an empty reserve must be dropped (idle-seal never seals empty)"
    );
    assert!(!h.segments_dir.join(format!("{id}.dat")).exists(), "no .dat for an empty reserve");
}

// ---------------------------------------------------------------------------
// Row 2: kill after data entries, before `.dat` fsync
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row2_kill_before_dat_fsync_replays_entries_and_reseals() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let (last_pos, data) = {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, b"aaaa").await;
        let last = h.append(id, 4, b"bbbb").await;
        h.crash().await;
        (last, b"aaaabbbb".to_vec())
    };
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // Folded state: Reserved-unsealed → replay entries, re-seal.
    assert_eq!(outcome.folded_segments, 1);
    assert_eq!(outcome.re_sealed_segments, 1);
    assert_eq!(outcome.adopted_segments, 0);
    assert_eq!(outcome.dropped_empty_reserves, 0);

    let entry = h.entry(id);
    assert_eq!(entry.state, SegmentState::Sealed, "re-sealed after replay");
    assert_eq!(
        entry.data_wal_pos,
        Some(last_pos),
        "the SealEvent must carry the LAST data entry's position"
    );
    assert_eq!(entry.metadata.merkle_root, root_fn(&data), "re-seal root matches the worker's");
    assert!(
        h.segments_dir.join(format!("{id}.dat")).exists(),
        "the replayed segment's .dat must be durable"
    );
}

// ---------------------------------------------------------------------------
// Row 3: kill after `.dat` fsync, before `SealEvent` — adopt, no re-seal I/O
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row3_kill_after_dat_fsync_before_seal_event_adopts() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let data = b"interrupted-seal-data".to_vec();
    let (last_pos, dat_mtime) = {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        let last = h.append(id, 0, &data).await;
        // The seal worker normally writes the .dat then appends the
        // SealEvent; simulate the crash between the two by writing the
        // durable .dat ourselves (header + data + empty index — the
        // same file layout the sealer produces) and NOT sealing.
        std::fs::create_dir_all(&h.segments_dir).unwrap();
        let header = crate::SegmentHeader::new(
            id,
            data.len() as u64,
            0,
            crate::segment::header::SEGMENT_HEADER_SIZE as u64 + data.len() as u64,
            *blake3::hash(&data).as_bytes(),
        );
        let mut raw = header.to_bytes();
        raw.extend_from_slice(&data);
        raw.extend_from_slice(&crate::SegmentIndex::new(vec![]).unwrap().to_bytes());
        let path = h.segments_dir.join(format!("{id}.dat"));
        std::fs::write(&path, &raw).unwrap();
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        h.crash().await;
        (last, mtime)
    };
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // Folded state: Reserved-unsealed (.dat orphan) → adopt: recompute
    // root, append SealEvent — no re-seal I/O.
    assert_eq!(outcome.folded_segments, 1);
    assert_eq!(outcome.adopted_segments, 1);
    assert_eq!(outcome.re_sealed_segments, 0, "adopt must not re-seal");
    assert_eq!(
        outcome.swept_entries, 1,
        "the adopted segment's WAL entry is garbage after adoption"
    );

    let entry = h.entry(id);
    assert_eq!(entry.state, SegmentState::Sealed);
    // The adopt's root covers the .dat's data section.
    assert_eq!(entry.metadata.merkle_root, root_fn(&data));
    assert_eq!(
        entry.data_wal_pos,
        Some(last_pos),
        "the adopted SealEvent carries the last data entry's position"
    );
    // The .dat was NOT rewritten (no re-seal I/O).
    let mtime_after =
        std::fs::metadata(h.segments_dir.join(format!("{id}.dat"))).unwrap().modified().unwrap();
    assert_eq!(mtime_after, dat_mtime, "adopt must not rewrite the .dat");
}

// ---------------------------------------------------------------------------
// Row 4: kill after `SealEvent`, before the data-WAL sweep
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row4_kill_after_seal_event_leaves_sealed_and_sweeps_entries() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, b"sealed-data").await;
        h.seal(id, b"sealed-data").await;
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // Folded state: Sealed → .dat authoritative; entries ≤ data_wal_pos
    // are swept (the pass counts the sealed segment's entries as swept).
    assert_eq!(outcome.folded_segments, 1);
    assert_eq!(outcome.re_sealed_segments, 0);
    assert_eq!(outcome.adopted_segments, 0);
    assert_eq!(outcome.swept_entries, 1, "the sealed segment's entry is swept, not replayed");
    assert_eq!(h.entry(id).state, SegmentState::Sealed);

    // The sweep rule: an entry at p ≤ data_wal_pos is garbage.
    let entry = h.entry(id);
    let pos = entry.data_wal_pos.expect("sealed entry records its last data position");
    assert!(entry_is_garbage(&entry, &pos), "entry at data_wal_pos is garbage after seal");
    assert!(
        !entry_is_garbage(&entry, &DataWalPos { file_seq: pos.file_seq, offset: pos.offset + 1 }),
        "an entry BEYOND data_wal_pos is live (the mutation check's protected direction)"
    );
}

// ---------------------------------------------------------------------------
// Row 5: kill after `DeleteEvent`, before `.dat` unlink
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row5_kill_after_delete_event_before_unlink_folds_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, b"to-be-deleted").await;
        h.seal(id, b"to-be-deleted").await;
        // The reaper would unlink the .dat AFTER the durable delete;
        // the crash happens before the unlink — the file survives.
        // (The direct request_seal wrote no file; write the .dat the
        // seal worker would have produced.)
        std::fs::create_dir_all(&h.segments_dir).unwrap();
        let header = crate::SegmentHeader::new(
            id,
            12,
            0,
            crate::segment::header::SEGMENT_HEADER_SIZE as u64 + 12,
            *blake3::hash(b"to-be-deleted").as_bytes(),
        );
        let mut raw = header.to_bytes();
        raw.extend_from_slice(b"to-be-deleted");
        raw.extend_from_slice(&crate::SegmentIndex::new(vec![]).unwrap().to_bytes());
        std::fs::write(h.segments_dir.join(format!("{id}.dat")), &raw).unwrap();
        h.delete(id).await;
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // Folded state: Deleted (immediate eviction with grace 0) → the
    // .dat is an orphan for the reaper to sweep; entries are garbage.
    assert_eq!(outcome.folded_segments, 1);
    assert_eq!(outcome.swept_entries, 1, "the deleted segment's entry is garbage");
    assert!(
        h.lifecycle.registry().get(id).is_none(),
        "delete with grace 0 evicts the entry (Deleted state folded)"
    );
    assert!(
        h.segments_dir.join(format!("{id}.dat")).exists(),
        "the .dat orphan survives for the reaper (unlink happens AFTER the delete)"
    );
}

// ---------------------------------------------------------------------------
// Row 6: unlink-before-delete is unrepresentable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row6_unlink_before_delete_is_unrepresentable() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, b"row6").await;
        h.seal(id, b"row6").await;
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    // Simulate the forbidden sequence's RESULT (a rogue actor removed
    // the file while the segment is still Sealed — no DeleteEvent).
    // The .dat may not exist (the direct request_seal wrote no file) —
    // the simulation's point is the MISSING file either way.
    let _ = std::fs::remove_file(h.segments_dir.join(format!("{id}.dat")));

    // The machine cannot express "unlink before delete":
    // (a) the API has no unlink operation — only the orphan reaper
    //     removes files, and its contract is request_delete FIRST;
    // (b) recovery must NOT fabricate a DeleteEvent for a missing file
    //     — the fold says Sealed (the event log is the only truth) and
    //     the row's "folded state" is a reaper responsibility, not a
    //     machine state.
    let outcome = h.recover().await;
    assert_eq!(outcome.folded_segments, 1);
    assert_eq!(h.entry(id).state, SegmentState::Sealed, "the fold cannot know about files");
    // (c) the delete transition itself requires a live entry: an
    //     attempted delete of a never-reserved id is rejected — the
    //     unlink-first sequence has no API to stand on.
    let rogue = SegmentId::new();
    let err =
        h.lifecycle.request_delete(rogue).await.expect_err("delete of an unknown id must fail");
    assert!(matches!(err, crate::segment::lifecycle::TransitionError::Missing));
}

// ---------------------------------------------------------------------------
// Invariant: reserve-before-entry (ADR-0024 Decision 1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn data_entry_without_reserve_is_swept_never_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        // A data entry for a segment whose ReserveEvent never landed (a
        // mutation / corruption — the invariant makes it impossible).
        h.sealer
            .append_wal_entry(WalEntry::new(
                id,
                0,
                6,
                6,
                0,
                0,
                0,
                HashOutput::from_bytes([0u8; 32]),
                Bytes::from_static(b"orphan"),
            ))
            .await
            .unwrap();
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    assert_eq!(outcome.folded_segments, 0, "no ReserveEvent → nothing folded");
    assert_eq!(outcome.swept_entries, 1, "the orphan entry is swept, never replayed");
    assert!(h.lifecycle.registry().get(id).is_none());
    assert!(!h.segments_dir.join(format!("{id}.dat")).exists(), "no .dat for an orphan");
}

// ---------------------------------------------------------------------------
// Event WAL checkpoint — byte-threshold trigger, restart cycle,
// replay bound, atomicity (ADR-0024 Decision 3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checkpoint_idle_log_produces_zero_checkpoints_and_burst_exactly_one() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot_ckpt(dir.path(), 1024).await; // huge threshold
        h.reserve(id).await;
        h.append(id, 0, b"idle").await;
        h.seal(id, b"idle").await;
        // Idle time must NEVER trigger (the threshold is the only
        // trigger — no time-based fallback).
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(h.checkpoint_file_count(), 0, "idle log must produce zero checkpoints");
        assert!(
            h.checkpoint.as_ref().unwrap().last_checkpoint_pos().is_none(),
            "no checkpoint may exist"
        );
        h.crash().await;
    }
    // A burst past the threshold produces exactly one checkpoint.
    let dir2 = tempfile::tempdir().unwrap();
    {
        let h = Harness::boot_ckpt(dir2.path(), 64).await; // one seal (80 B) crosses it
        h.reserve(id).await;
        h.append(id, 0, b"burst").await;
        h.seal(id, b"burst").await; // 36 + 80 = 116 >= 64 → triggers
        h.wait_until(|h| h.checkpoint_file_count() == 1).await;
        // The latch: no second checkpoint spawns from the same burst.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(h.checkpoint_file_count(), 1, "a burst must produce exactly one checkpoint");
        h.crash().await;
    }
}

#[tokio::test]
async fn checkpoint_full_cycle_threshold_trigger_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let sealed_id = SegmentId::new();
    let reserved_id = SegmentId::new();
    let (last_pos, sealed_root) = {
        let h = Harness::boot_ckpt(dir.path(), 128).await;
        h.reserve(sealed_id).await;
        let pos = h.append(sealed_id, 0, b"checkpoint-sealed").await;
        h.seal(sealed_id, b"checkpoint-sealed").await;
        h.reserve(reserved_id).await; // pushes past the threshold (36+80+36 = 152 >= 128)
        h.wait_until(|h| h.checkpoint.as_ref().unwrap().last_checkpoint_pos().is_some()).await;
        assert_eq!(h.checkpoint_file_count(), 1);
        h.crash().await;
        (pos, root_fn(b"checkpoint-sealed"))
    };

    // Restart: load the checkpoint → seed → fold from the covered
    // position → data-WAL pass.
    let h = Harness::boot_ckpt(dir.path(), 128).await;
    let (snapshot, covered) =
        h.checkpoint.as_ref().unwrap().load_checkpoint().unwrap().expect("checkpoint loads");

    h.lifecycle.seed_from_checkpoint(&snapshot);
    let outcome = h.recover_from(covered).await;

    // Machine state equals the pre-crash state (the snapshot carried it):
    // the sealed segment is Sealed with its root and data_wal_pos (the
    // retention input) surviving the checkpoint.
    let entry = h.entry(sealed_id);
    assert_eq!(entry.state, SegmentState::Sealed);
    assert_eq!(entry.metadata.merkle_root, sealed_root);
    assert_eq!(entry.data_wal_pos, Some(last_pos), "data_wal_pos survives checkpointing");
    // The empty reserve (Reserved, no data entries) is dropped by the
    // data-WAL pass.
    assert!(h.lifecycle.registry().get(reserved_id).is_none(), "empty reserve dropped");
    // The fold after the covered position reads only events whose
    // folds had NOT landed when the checkpoint fired: the checkpoint
    // covers the last FOLDED position (never the raw WAL tail — a
    // tail-covering checkpoint would seed a snapshot missing
    // appended-but-unfolded segments and abort restart). The final
    // reserve was appended after the trigger; it re-folds idempotently
    // (Reserve on the snapshot's Reserved entry).
    assert_eq!(
        outcome.folded_segments, 1,
        "the post-covered reserve re-folds idempotently (the checkpoint covers folded events only)"
    );
    // Retention stays correct after the checkpoint: the sealed segment's
    // data-WAL entries are swept by the position rule (the sweep ran
    // inside the recovery pass, using data_wal_pos from the snapshot).
    assert_eq!(outcome.swept_entries, 1, "the sealed segment's entry is swept post-checkpoint");
    assert!(h.event_wal.bytes_since(covered) <= 128, "replay bound holds after restart");
    // The event log is the only durable writer (ADR-0025 Decision 3
    // final form — no CF mirror exists to compare).
}

#[tokio::test]
async fn checkpoint_replay_bound_is_independent_of_total_volume() {
    let dir = tempfile::tempdir().unwrap();
    // 10× the threshold volume of events, then a checkpoint at the end
    // of the burst: the startup fold must read only the post-covered
    // events (≤ the threshold), regardless of the lifetime volume.
    let h = Harness::boot_ckpt(dir.path(), 1024).await;
    let mut ids = Vec::new();
    for i in 0..10u64 {
        let id = SegmentId::new();
        h.reserve(id).await;
        h.append(id, 0, format!("volume-{i}").as_bytes()).await;
        h.seal(id, format!("volume-{i}").as_bytes()).await;
        ids.push(id);
    }
    // The burst crossed the threshold several times; let the last
    // checkpoint settle, then snapshot the current covered position.
    h.wait_until(|h| h.checkpoint_file_count() >= 1).await;
    let registry = h.lifecycle.registry();
    // A synthetic snapshot at the CURRENT tail (deterministic covered
    // position — the trigger's async task may still be mid-flight).
    let covered = h.event_wal.latest_pos();
    h.checkpoint.as_ref().unwrap().write_checkpoint(registry, covered).unwrap();
    h.checkpoint.as_ref().unwrap().truncate_before(covered).await.unwrap();

    // Restart: fold from the covered position — the fold reads at most
    // the threshold's worth of events (here: none — everything is
    // covered).
    h.crash().await;
    let h = Harness::boot_ckpt(dir.path(), 1024).await;
    let (snapshot, loaded_covered) =
        h.checkpoint.as_ref().unwrap().load_checkpoint().unwrap().expect("checkpoint loads");
    assert_eq!(loaded_covered, covered);
    h.lifecycle.seed_from_checkpoint(&snapshot);
    let outcome = h.recover_from(loaded_covered).await;
    assert!(
        h.event_wal.bytes_since(loaded_covered) <= 1024,
        "startup fold is bounded by the threshold, not by lifetime volume"
    );
    // Every pre-crash segment is Sealed with its root.
    for id in &ids {
        let entry = h.entry(*id);
        assert_eq!(entry.state, SegmentState::Sealed);
    }
    assert_eq!(outcome.folded_segments, 0);
}

#[tokio::test]
async fn checkpoint_atomicity_crash_during_temp_write_recovers_from_old_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let root = {
        let h = Harness::boot_ckpt(dir.path(), 1024).await;
        h.reserve(id).await;
        h.append(id, 0, b"atomic-1").await;
        h.seal(id, b"atomic-1").await;
        // A first checkpoint (the "old" one).
        let covered = h.event_wal.latest_pos();
        h.checkpoint.as_ref().unwrap().write_checkpoint(h.lifecycle.registry(), covered).unwrap();
        h.checkpoint.as_ref().unwrap().truncate_before(covered).await.unwrap();
        h.crash().await;
        root_fn(b"atomic-1")
    };
    // Crash DURING the next temp write: an orphan .tmp appears; the old
    // checkpoint + full fold remain the recovery path.
    std::fs::write(dir.path().join("event-wal/checkpoint-00000000-999.tmp"), b"partial").unwrap();

    let h = Harness::boot_ckpt(dir.path(), 1024).await;
    let (snapshot, covered) =
        h.checkpoint.as_ref().unwrap().load_checkpoint().unwrap().expect("old checkpoint loads");
    h.lifecycle.seed_from_checkpoint(&snapshot);
    let _ = h.recover_from(covered).await;
    assert!(
        !dir.path().join("event-wal/checkpoint-00000000-999.tmp").exists(),
        "orphan .tmp cleaned at load"
    );
    let entry = h.entry(id);
    assert_eq!(entry.state, SegmentState::Sealed);
    assert_eq!(entry.metadata.merkle_root, root);
}

#[tokio::test]
async fn checkpoint_atomicity_crash_after_rename_before_truncate_folds_from_covered() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let covered = {
        let h = Harness::boot_ckpt(dir.path(), 1024).await;
        h.reserve(id).await;
        h.append(id, 0, b"atomic-2").await;
        h.seal(id, b"atomic-2").await;
        // The checkpoint's rename lands, but the crash happens BEFORE
        // truncate_before: the covered events are still in the log.
        let covered = h.event_wal.latest_pos();
        h.checkpoint.as_ref().unwrap().write_checkpoint(h.lifecycle.registry(), covered).unwrap();
        h.crash().await; // no truncate
        covered
    };
    let h = Harness::boot_ckpt(dir.path(), 1024).await;
    let (snapshot, loaded_covered) =
        h.checkpoint.as_ref().unwrap().load_checkpoint().unwrap().expect("new checkpoint loads");
    assert_eq!(loaded_covered, covered);
    h.lifecycle.seed_from_checkpoint(&snapshot);
    let outcome = h.recover_from(loaded_covered).await;
    // Re-folding the covered events is impossible by construction: the
    // fold starts at the covered position.
    assert_eq!(outcome.folded_segments, 0);
    let entry = h.entry(id);
    assert_eq!(entry.state, SegmentState::Sealed);
    assert_eq!(entry.metadata.merkle_root, root_fn(b"atomic-2"));
}

// ---------------------------------------------------------------------------
// Adopt fallback: a .dat with a truncated data section must fall back
// to WAL replay (never silently adopted-then-truncated)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn truncated_dat_falls_back_to_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let data = b"truncated-dat-fallback".to_vec();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, &data).await;
        // A .dat whose header is valid but whose data section is cut
        // short (disk corruption — the atomic rename normally prevents
        // this): the header claims `data.len()` bytes but the file has
        // fewer.
        std::fs::create_dir_all(&h.segments_dir).unwrap();
        let header = crate::SegmentHeader::new(
            id,
            data.len() as u64,
            0,
            crate::segment::header::SEGMENT_HEADER_SIZE as u64 + data.len() as u64,
            *blake3::hash(&data).as_bytes(),
        );
        let mut raw = header.to_bytes();
        raw.extend_from_slice(&data[..5]); // truncated data section
        std::fs::write(h.segments_dir.join(format!("{id}.dat")), &raw).unwrap();
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // The truncated .dat must NOT be adopted: the segment is replayed
    // from the WAL (its entries were buffered during the stream), and
    // the re-seal overwrites the corrupt file.
    assert_eq!(outcome.adopted_segments, 0, "a truncated .dat must not be adopted");
    assert_eq!(outcome.re_sealed_segments, 1, "the segment is rebuilt from the WAL instead");
    let entry = h.entry(id);
    assert_eq!(entry.state, SegmentState::Sealed);
    assert_eq!(entry.metadata.merkle_root, root_fn(&data));
}

// ---------------------------------------------------------------------------
// Invariant: torn tail truncates the fold at the last good record
// ---------------------------------------------------------------------------

#[tokio::test]
async fn torn_tail_truncates_the_fold_at_the_last_good_record() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    // Tear the tail AFTER open (open truncates pre-existing torn tails):
    // a partial second record now sits at the end of the log.
    h.event_wal
        .append(SegmentEvent::Delete(crate::segment::event_wal::DeleteEvent { segment_id: id }))
        .await
        .unwrap();
    let path = dir.path().join("event-wal/evl_00000000.log");
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(36 + 10).unwrap(); // partial header of the delete record
    drop(file);

    let outcome = h.recover().await;
    // The fold stopped at the last good record (the reserve); the
    // partial delete record was ignored — recovery proceeds, no abort.
    assert_eq!(outcome.folded_segments, 1);
    assert_eq!(outcome.dropped_empty_reserves, 1, "the folded reserve is dropped (empty)");
    assert!(h.lifecycle.registry().get(id).is_none());
}

// ---------------------------------------------------------------------------
// Invariant: fold determinism + dual-read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fold_is_deterministic_and_reproduces_the_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, b"det").await;
        h.seal(id, b"det").await;
        h.crash().await;
    }
    // Fold the same events twice into fresh registries — identical
    // states (the DoD's order-exact determinism).
    let h = Harness::boot(dir.path()).await;
    let _ = h.recover().await; // fold + dual-read + pass (empty residue)
    let a = h.lifecycle.registry().get(id).unwrap();
    h.crash().await; // the second fold must start from the on-disk state

    let h2 = Harness::boot(dir.path()).await;
    let _ = h2.recover().await; // dual-read verify runs inside
    let b = h2.lifecycle.registry().get(id).unwrap();
    assert_eq!(a.state, b.state);
    assert_eq!(a.metadata.segment_id, b.metadata.segment_id);
    assert_eq!(a.metadata.merkle_root, b.metadata.merkle_root);
    assert_eq!(a.data_wal_pos, b.data_wal_pos);

    // The event log is the only durable writer (no CF mirror).
}

// ---------------------------------------------------------------------------
// Invariant: mid-log corruption aborts with the record position
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mid_log_corruption_aborts_the_fold_with_the_position() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        // Corrupt the SECOND record (a seal for another segment) so a
        // valid record follows it.
        h.event_wal
            .append(SegmentEvent::Seal(crate::segment::event_wal::SealEvent {
                pool_id: 0,
                segment_id: id,
                tier: SizeTier::Small,
                ec_k: 4,
                ec_m: 2,
                merkle_root: HashOutput::from_bytes([0xAB; 32]),
                data_wal_pos: DataWalPos { file_seq: 0, offset: 0 },
                repacked_from: None,
            }))
            .await
            .unwrap();
        // A THIRD record after the one we corrupt: valid data follows,
        // so the corruption is mid-log, not a torn tail.
        h.event_wal
            .append(SegmentEvent::Delete(crate::segment::event_wal::DeleteEvent { segment_id: id }))
            .await
            .unwrap();
        h.crash().await;
    }
    // Flip a byte inside the second record (reserve = 36 bytes).
    let path = dir.path().join("event-wal/evl_00000000.log");
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    use std::io::{Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(36 + 30)).unwrap();
    file.write_all(&[0xFF]).unwrap();
    drop(file);

    let h = Harness::boot(dir.path()).await;
    let reader = WalReader::open(&WalConfig {
        data_dir: dir.path().join("wal"),
        max_file_size_bytes: 64 * 1024 * 1024,
        fsync_batch_timeout_ms: 5,
        wal_use_sync_file_range: false,
    })
    .unwrap();
    let result = h
        .lifecycle
        .rebuild_with_data_wal(
            h.event_wal.read_from(EventWalPos { file_seq: 0, offset: 0 }),
            &reader,
            &h.sealer,
            root_fn,
            &h.data_wal,
        )
        .await;
    match result.expect_err("mid-log corruption must abort") {
        crate::Error::CorruptEventLog { pos, .. } => {
            assert_eq!(pos.offset, 36, "abort must carry the corrupt record's position")
        }
        other => panic!("expected CorruptEventLog, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// entry_is_garbage — the sweep boundary (unit-level mutation checks)
// ---------------------------------------------------------------------------

#[test]
fn entry_is_garbage_position_boundary() {
    let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
    let id = SegmentId::new();
    let reserved_meta = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Small,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: None,
    };
    registry.reserve(id, reserved_meta).unwrap();
    let entry = registry.get(id).unwrap();
    let pos = DataWalPos { file_seq: 1, offset: 100 };
    assert!(!entry_is_garbage(&entry, &pos), "Reserved is always live");
}

#[test]
fn entry_is_garbage_sealed_position_rule_and_mutation_checks() {
    let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
    let id = SegmentId::new();
    let sealed_meta = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Small,
        merkle_root: Some(HashOutput::from_bytes([0xAB; 32])),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1_700_000_000_000),
    };
    registry
        .seal(id, sealed_meta.clone())
        .expect_err("seal of an absent id is rejected — the fold's corruption path");
    // A reserved entry, sealed through the typed API.
    registry
        .reserve(
            id,
            SegmentMetadata {
                pool_id: 0,
                total_bytes: 0,
                segment_id: id,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Small,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: None,
            },
        )
        .unwrap();
    // Record the last data position (what the write path does).
    registry.record_data_wal_pos(id, DataWalPos { file_seq: 2, offset: 500 });
    registry.seal(id, sealed_meta).unwrap();
    let entry = registry.get(id).unwrap();

    // The position rule: garbage iff data_wal_pos ≥ p.
    assert!(
        entry_is_garbage(&entry, &DataWalPos { file_seq: 2, offset: 500 }),
        "p == data_wal_pos"
    );
    assert!(entry_is_garbage(&entry, &DataWalPos { file_seq: 2, offset: 100 }), "p < data_wal_pos");
    assert!(
        !entry_is_garbage(&entry, &DataWalPos { file_seq: 2, offset: 501 }),
        "p > data_wal_pos"
    );

    // Mutation checks (DoD): an off-by-one data_wal_pos changes the
    // sweep boundary.
    // (a) TOO SMALL (data_wal_pos - 1): an entry that must be swept (at
    //     the true boundary) is protected → the bounded-protection
    //     invariant fails (files pinned forever).
    let mut too_small = registry.get(id).unwrap();
    too_small.data_wal_pos = Some(DataWalPos { file_seq: 2, offset: 499 });
    assert!(
        !entry_is_garbage(&too_small, &DataWalPos { file_seq: 2, offset: 500 }),
        "off-by-one-too-small protects a swept entry (the leak direction)"
    );
    // (b) TOO LARGE (data_wal_pos + 1): an entry beyond the true
    //     boundary is swept → a live entry is destroyed.
    let mut too_large = registry.get(id).unwrap();
    too_large.data_wal_pos = Some(DataWalPos { file_seq: 2, offset: 501 });
    assert!(
        entry_is_garbage(&too_large, &DataWalPos { file_seq: 2, offset: 501 }),
        "off-by-one-too-large sweeps an entry that must stay live"
    );
}

#[test]
fn entry_is_garbage_deleted_is_always_garbage() {
    let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
    let id = SegmentId::new();
    registry
        .reserve(
            id,
            SegmentMetadata {
                pool_id: 0,
                total_bytes: 0,
                segment_id: id,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Small,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: None,
            },
        )
        .unwrap();
    // With the default grace 0 the entry is evicted immediately; use a
    // grace config to observe the Deleted state.
    registry.delete(id).unwrap();
    let grace = SegmentLifecycleRegistry::new(&LifecycleConfig {
        lifecycle_registry_shards: 8,
        delete_grace_ms: 1000,
    });
    grace
        .reserve(
            id,
            SegmentMetadata {
                pool_id: 0,
                total_bytes: 0,
                segment_id: id,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Small,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: None,
            },
        )
        .unwrap();
    grace.delete(id).unwrap();
    let entry = grace.get(id).unwrap();
    assert_eq!(entry.state, SegmentState::Deleted);
    assert!(
        entry_is_garbage(&entry, &DataWalPos { file_seq: 0, offset: 0 }),
        "Deleted entries are always garbage"
    );
}

/// Recursively copies a directory tree (the deterministic-rebuild
/// test's "identical on-disk state" — the harness dirs hold RocksDB +
/// event WAL + data WAL files).
fn copy_dir_tree(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Startup-rebuild DoD: rebuild from identical on-disk state always
/// produces identical registry state and an identical `RebuildOutcome`
/// (the deterministic rebuild invariant — state = fold(events), nothing
/// else).
#[tokio::test]
async fn rebuild_is_deterministic_across_copied_data_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let id2 = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, b"deterministic").await;
        h.seal(id, b"deterministic").await;
        // A second reserved-unsealed segment exercises the data-WAL pass.
        h.reserve(id2).await;
        h.append(id2, 0, b"residue").await;
        h.crash().await;
    }

    // Two identical copies of the on-disk state.
    let dir2 = tempfile::tempdir().unwrap();
    copy_dir_tree(dir.path(), dir2.path());
    let h1 = Harness::boot(dir.path()).await;
    let h2 = Harness::boot(dir2.path()).await;
    let o1 = h1.recover().await;
    let o2 = h2.recover().await;

    // Identical outcome vectors.
    assert_eq!(o1.folded_segments, o2.folded_segments, "folded must match");
    assert_eq!(o1.dropped_empty_reserves, o2.dropped_empty_reserves);
    assert_eq!(o1.re_sealed_segments, o2.re_sealed_segments);
    assert_eq!(o1.adopted_segments, o2.adopted_segments);
    assert_eq!(o1.swept_entries, o2.swept_entries);

    // Identical registry state for every segment.
    for (sid, label) in [(id, "sealed"), (id2, "residue")] {
        let e1 = h1.entry(sid);
        let e2 = h2.entry(sid);
        assert_eq!(e1.state, e2.state, "{label} state must match");
        assert_eq!(e1.data_wal_pos, e2.data_wal_pos, "{label} position must match");
        assert_eq!(e1.metadata.merkle_root, e2.metadata.merkle_root, "{label} root must match");
    }
}

/// Startup-rebuild regression (SUT double-delete): two concurrent
/// `request_delete`s can both validate and append their `DeleteEvent`
/// — the first fold evicts, the second event is durable. The recovery
/// fold must treat the duplicate delete as a no-op, not abort startup
/// (a crash after the double-append previously bricked the node with
/// `EventFoldError: segment not present in the lifecycle registry`).
#[tokio::test]
async fn duplicate_delete_event_folds_as_noop() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.append(id, 0, b"double-delete").await;
        h.seal(id, b"double-delete").await;
        h.delete(id).await; // valid delete: appends DeleteEvent #1, folds + evicts
                            // The racing second delete: both validated before either folded —
                            // its DeleteEvent is durable even though the fold of #1 evicted
                            // the entry.
        h.event_wal
            .append(crate::segment::event_wal::SegmentEvent::Delete(
                crate::segment::event_wal::DeleteEvent { segment_id: id },
            ))
            .await
            .unwrap();
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // The fold must not abort: the duplicate delete is a no-op.
    assert_eq!(outcome.folded_segments, 1, "the reserve+seal+2 deletes fold");
    assert!(h.lifecycle.registry().get(id).is_none(), "the segment stays deleted + evicted");
}

/// Startup-rebuild regression (SUT 30-min run): a segment whose
/// `SealEvent` lands AFTER its `DeleteEvent` (the compactor's seal
/// append failed after partial durability, its cleanup deleted the
/// Reserved replacement, and the seal record hit the log afterwards —
/// Reserve → Delete → Seal). The fold must let the delete win and
/// treat the seal as a no-op — not abort startup.
#[tokio::test]
async fn seal_after_delete_folds_as_noop() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    {
        let h = Harness::boot(dir.path()).await;
        h.reserve(id).await;
        h.delete(id).await; // e.g. the compactor's cleanup_reserved_new
                            // The racing seal: its event append was in flight when the
                            // delete landed — the seal record is durable AFTER the delete.
        h.event_wal
            .append(crate::segment::event_wal::SegmentEvent::Seal(
                crate::segment::event_wal::SealEvent {
                    pool_id: 0,
                    segment_id: id,
                    tier: oceanfs_core::SizeTier::Standard,
                    ec_k: 4,
                    ec_m: 2,
                    merkle_root: oceanfs_core::HashOutput::from_bytes([0xAA; 32]),
                    data_wal_pos: crate::segment::event_wal::DataWalPos { file_seq: 0, offset: 0 },
                    repacked_from: None,
                },
            ))
            .await
            .unwrap();
        h.crash().await;
    }
    let h = Harness::boot(dir.path()).await;
    let outcome = h.recover().await;

    // The fold must not abort; the delete wins (the entry stays gone).
    assert_eq!(outcome.folded_segments, 1, "the reserve+delete+seal folds");
    assert!(h.lifecycle.registry().get(id).is_none(), "the segment stays deleted + evicted");
}
