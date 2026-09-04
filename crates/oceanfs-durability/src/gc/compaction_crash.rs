//! Compaction crash-window fault-injection matrix — rows 7–9 of
//! ADR-0025 §Crash-window table (test contract, not documentation).
//!
//! Each test drives the compactor to a milestone through the
//! `stall_seam` (the compactor stalls between milestones), **kills the
//! process** (aborts the compaction task and drops every instance — the
//! on-disk state is all that survives), reboots, runs the startup
//! sequence (fold + data-WAL pass + `recover_incomplete_compactions`),
//! and asserts (a) the folded state exactly matches the table's
//! "Folded state" column, (b) the recovery action column, (c) reads
//! resolve correctly (objects point at new or old per milestone).
//!
//! | Crash between | Folded state | Recovery action |
//! |---|---|---|
//! | Copying: reserve → `.dat` | Reserved, no data | Dropped (row 1 — the data-WAL pass) |
//! | Copying: `.dat` → `SealEvent(new)` | Reserved-unsealed (`.dat` orphan) | Adopted (row 3 — the data-WAL pass) |
//! | NewSealed → ObjectsMoved | New sealed, objects→old | `SweepNewOrphan(new)` (row 7) |
//! | ObjectsMoved → OldDeleted | Objects→new, old sealed | `FinishOldDeletion(old)` (row 8) |
//! | OldDeleted → OldRemoved | Old deleted, `.dat` present | `SweepOldDat(old)` (row 9) |
//!
//! The metadata-only-compaction window (a `SealEvent(new)` with no
//! durable `.dat`) is **unrepresentable**: the compactor orders the
//! `.dat` write before `request_seal`, so a kill before the write
//! leaves the fold at `Copying` (no event) and a kill after leaves the
//! `.dat` on disk — the table's windows are the only ones reachable.

#![cfg(test)]
#![allow(
    clippy::disallowed_types,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_borrow,
    clippy::await_holding_lock
)]

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use oceanfs_core::{
    BucketId, ChunkRef, EventWalConfig, HashOutput, LifecycleConfig, MetadataConfig, ObjectKey,
    ObjectMetadata, SegmentId, SegmentMetadata, SizeTier, WalConfig,
};
use oceanfs_storage::{
    metadata::RocksDbMetadataStore,
    segment::{
        event_wal::{EventWal, EventWalPos},
        lifecycle::{RebuildOutcome, SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
        TierRouter,
    },
    wal::{WalReader, WalWriter},
    SealConfig, SegmentSealer,
};

use super::{
    compaction_recovery::{recover_incomplete_compactions, CompactionRecoveryAction, ObjectLookup},
    garbage_collector::{DiskSegmentShardStore, SegmentShardStore},
    segment_compactor::{stall_seam, SegmentCompactor},
};
use crate::{anti_entropy::SegmentDataStore, segment_store_impl::DiskSegmentStore};

/// The deterministic merkle-root builder shared by the compactor (its
/// seal-time root), the seed helper, and the recovery pass: the
/// Merkle root over the data section (the node's construction).
fn root_fn(data: &[u8]) -> Option<HashOutput> {
    crate::MerkleTree::build(data, 0).map(|tree| tree.root().hash())
}

/// Serializes the stall-seam tests: the seam's `STALL_AT`/`REACHED`
/// statics are process-global, so concurrently running tests
/// cross-talk (one harness can observe another test's milestone and
/// kill its compactor at the wrong point — flaky rows 7-9 under
/// parallel execution). Each test holds the lock for its whole body;
/// the guard drops on unwind, so a panic can never deadlock the rest.
static SEAM_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A fully wired GC slice — store (objects + CF mirror), event wal,
/// data wal, coordinator, sealer, data store, shard store, compactor.
struct Harness {
    store: Arc<RocksDbMetadataStore>,
    event_wal: Arc<EventWal>,
    data_wal: Arc<WalWriter>,
    registry: Arc<SegmentLifecycleRegistry>,
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    sealer: Arc<SegmentSealer>,
    data_store: Arc<DiskSegmentStore>,
    shard_store: Arc<DiskSegmentShardStore>,
    compactor: Arc<SegmentCompactor>,
    segments_dir: PathBuf,
    wal_dir: PathBuf,
}

impl Harness {
    /// "Boots" the GC slice on `dir` (fresh or reopened).
    async fn boot(dir: &Path) -> Harness {
        let store = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        let event_wal_config = EventWalConfig {
            event_wal_dir: dir.join("event-wal"),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024 * 1024,
        };
        let event_wal = Arc::new(
            EventWal::open(event_wal_config.event_wal_dir.clone(), &event_wal_config)
                .await
                .unwrap(),
        );
        let wal_config = WalConfig {
            data_dir: dir.join("wal"),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            wal_use_sync_file_range: false,
        };
        let data_wal = Arc::new(WalWriter::open(&wal_config).await.unwrap());

        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&registry))
                .with_event_wal(event_wal.clone()),
        );
        // Pools-only harness (ADR-0031): one data pool whose root IS the
        // segments dir — stores, sealer and the `.dat` path assertions
        // all keep resolving to `dir/segments` unchanged. The data pool
        // is configured first (config-order id 0).
        let segments_dir = dir.join("segments");
        for pool_root in [
            segments_dir.clone(),
            dir.join("pool-wal"),
            dir.join("pool-meta"),
            dir.join("pool-hints"),
        ] {
            std::fs::create_dir_all(pool_root).expect("pool root");
        }
        let storage = oceanfs_core::StorageConfig {
            pools: vec![
                oceanfs_core::StoragePoolConfig {
                    name: "data-0".into(),
                    role: oceanfs_core::PoolRole::Data,
                    root: segments_dir.clone(),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                oceanfs_core::StoragePoolConfig {
                    name: "wal-0".into(),
                    role: oceanfs_core::PoolRole::Wal,
                    root: dir.join("pool-wal"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                oceanfs_core::StoragePoolConfig {
                    name: "meta-0".into(),
                    role: oceanfs_core::PoolRole::Metadata,
                    root: dir.join("pool-meta"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                oceanfs_core::StoragePoolConfig {
                    name: "hints-0".into(),
                    role: oceanfs_core::PoolRole::Hints,
                    root: dir.join("pool-hints"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        };
        let pool_registry = oceanfs_storage::PoolRegistry::from_config(&storage, &dir.join("data"))
            .expect("pool registry");
        let data_pools = pool_registry.data_pools();
        assert_eq!(data_pools[0].id(), 0, "data-first config-order id");
        let data_store = Arc::new(DiskSegmentStore::new(data_pools.clone(), Arc::new(|_| Some(0))));
        let shard_store =
            Arc::new(DiskSegmentShardStore::new(data_pools.clone(), Arc::new(|_| Some(0))));
        let sealer = Arc::new(SegmentSealer::new(
            SealConfig { data_dir: segments_dir.clone(), ..Default::default() },
            data_wal.clone(),
            lifecycle.clone(),
        ));
        let compactor = Arc::new(SegmentCompactor::new(
            store.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
            data_store.clone(),
            lifecycle.clone(),
            shard_store.clone(),
        ));

        Harness {
            store,
            event_wal,
            data_wal,
            registry,
            lifecycle,
            sealer,
            data_store,
            shard_store,
            compactor,
            segments_dir,
            wal_dir: wal_config.data_dir,
        }
    }

    /// Seeds a Sealed old segment with a durable `.dat` (the shape the
    /// write path produces: reserve → data → fsync → seal).
    async fn seed_sealed_old(&self, id: SegmentId, data: &[u8]) {
        self.lifecycle.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        self.data_store.write_segment_data(&id, data).unwrap();
        let meta = SegmentMetadata {
            pool_id: 0,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: root_fn(data),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1_700_000_000_000),
        };
        self.lifecycle.request_seal(id, meta, None).await.unwrap();
    }

    /// Puts an object with a single chunk reference.
    fn put_object(&self, key: &str, chunk: ChunkRef) {
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(chunk);
        self.store
            .put_object_in_bucket(
                &BucketId::new("default"),
                ObjectMetadata {
                    object_key: ObjectKey::new(key),
                    size: chunk.logical_length as u64,
                    blake3_hash: None,
                    chunks,
                    inline_data: None,
                    created_at: 0,
                    hlc: oceanfs_core::Hlc::zero(),
                },
            )
            .unwrap();
    }

    /// Returns the object's first chunk reference.
    fn object_chunk(&self, key: &str) -> ChunkRef {
        self.store
            .get_object(&BucketId::new("default"), &ObjectKey::new(key))
            .unwrap()
            .expect("object exists")
            .chunks
            .first()
            .copied()
            .expect("object has a chunk")
    }

    /// The startup sequence on the REBOOTED slice: fold the event log,
    /// run the data-WAL pass, then `recover_incomplete_compactions` —
    /// returns the outcome vector and the compaction recovery actions.
    async fn recover(&self) -> (RebuildOutcome, Vec<CompactionRecoveryAction>) {
        let reader = WalReader::open(&WalConfig {
            data_dir: self.wal_dir.clone(),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            wal_use_sync_file_range: false,
        })
        .unwrap();
        let outcome = self
            .lifecycle
            .rebuild_with_data_wal(
                self.event_wal.read_from(EventWalPos { file_seq: 0, offset: 0 }),
                &reader,
                &self.sealer,
                root_fn,
                &self.data_wal,
            )
            .await
            .unwrap();
        let actions = recover_incomplete_compactions(
            &self.lifecycle.registry(),
            &StoreLookup { store: self.store.clone() },
        )
        .unwrap();
        (outcome, actions)
    }

    /// Spawns the compaction and waits (bounded) for the seam to report
    /// `milestone` reached.
    async fn drive_to_milestone(
        &self,
        old_id: SegmentId,
        old_meta: SegmentMetadata,
        milestone: u8,
    ) -> tokio::task::JoinHandle<()> {
        stall_seam::arm(milestone);
        let compactor = Arc::clone(&self.compactor);
        let handle = tokio::spawn(async move {
            let _ = compactor.compact_segment(old_id, &old_meta, &HashSet::new()).await;
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while stall_seam::REACHED.load(Ordering::SeqCst) != milestone {
            assert!(Instant::now() < deadline, "compaction did not reach milestone {milestone}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle
    }

    /// Releases the current stall and waits for the next milestone.
    async fn advance_to(&self, next: u8) {
        stall_seam::arm(next);
        let deadline = Instant::now() + Duration::from_secs(10);
        while stall_seam::REACHED.load(Ordering::SeqCst) != next {
            assert!(Instant::now() < deadline, "compaction did not reach milestone {next}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Kills the stalled compaction task and disarms the seam.
    async fn kill(&self, handle: tokio::task::JoinHandle<()>) {
        handle.abort();
        let _ = handle.await;
        stall_seam::disarm();
    }

    fn entry(&self, id: SegmentId) -> oceanfs_storage::segment::lifecycle::LifecycleEntry {
        self.lifecycle.registry().get(id).expect("registry entry present")
    }
}

/// The production-shaped objects-CF lookup: one scan answers whether any
/// object references a segment.
struct StoreLookup {
    store: Arc<RocksDbMetadataStore>,
}

impl ObjectLookup for StoreLookup {
    fn is_referenced(&self, segment_id: SegmentId) -> crate::Result<bool> {
        Ok(self
            .store
            .list_objects_all_with_bucket()
            .into_iter()
            .flatten()
            .any(|(_, obj)| obj.chunks.iter().any(|c| c.segment_id == segment_id)))
    }
}

// ---------------------------------------------------------------------------
// Pre-NewSealed windows (rows 1 and 3 behavior on compaction units)
// ---------------------------------------------------------------------------

/// Kill between the reserve and the `.dat` write: the fold shows the
/// new segment `Reserved` with no data — the data-WAL pass drops the
/// empty reserve (row 1); the old segment is untouched.
#[tokio::test]
async fn kill_before_dat_write_folds_copying_and_drops_the_reserve() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let old_data = vec![0xEE; 400];
    let old_meta = {
        let h = Harness::boot(dir.path()).await;
        h.seed_sealed_old(old_id, &old_data).await;
        h.put_object(
            "a.txt",
            ChunkRef {
                segment_id: old_id,
                offset: 0,
                length: 400,
                compressed: false,
                logical_length: 400,
            },
        );
        h.entry(old_id).metadata.clone()
    };
    let h = Harness::boot(dir.path()).await;
    let handle = h.drive_to_milestone(old_id, old_meta, 1).await;
    h.kill(handle).await;
    drop(h);

    let h2 = Harness::boot(dir.path()).await;
    let (outcome, actions) = h2.recover().await;

    // Folded: the new reserve has no `.dat` and no WAL entries → dropped
    // (the table's "Reserved, empty" row); the old segment is intact.
    assert_eq!(outcome.dropped_empty_reserves, 1, "the empty reserve is dropped");
    assert_eq!(h2.entry(old_id).state, oceanfs_storage::segment::lifecycle::SegmentState::Sealed);
    assert_eq!(h2.object_chunk("a.txt").segment_id, old_id, "objects still point at the old");
    assert!(actions.is_empty(), "no marked unit exists — a reserve has no SealEvent yet");
}

/// Kill between the `.dat` write and the `SealEvent(new)`: the fold
/// shows the new segment `Reserved` with a durable `.dat` — the
/// data-WAL pass adopts it (row 3, no re-seal I/O). The adopted
/// replacement carries no `repacked_from` marker (adoption doesn't know
/// the unit), so recovery returns no action; the unreferenced
/// replacement is reaped like any orphan.
#[tokio::test]
async fn kill_between_dat_write_and_seal_adopts_the_new_dat() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let old_data = vec![0xEE; 400];
    let old_meta = {
        let h = Harness::boot(dir.path()).await;
        h.seed_sealed_old(old_id, &old_data).await;
        h.put_object(
            "a.txt",
            ChunkRef {
                segment_id: old_id,
                offset: 0,
                length: 400,
                compressed: false,
                logical_length: 400,
            },
        );
        h.entry(old_id).metadata.clone()
    };
    let h = Harness::boot(dir.path()).await;
    let handle = h.drive_to_milestone(old_id, old_meta, 2).await;
    h.kill(handle).await;
    drop(h);

    let h2 = Harness::boot(dir.path()).await;
    let (outcome, actions) = h2.recover().await;

    assert_eq!(outcome.adopted_segments, 1, "the durable .dat is adopted (row 3)");
    assert_eq!(h2.object_chunk("a.txt").segment_id, old_id, "objects still point at the old");
    // The new segment: Sealed via adoption, marker lost (documented:
    // the reaper's orphan scan reaps the unreferenced replacement).
    let mut ids: Vec<SegmentId> = Vec::new();
    h2.lifecycle.registry().for_each(|id, _entry| ids.push(id));
    let new_id = ids
        .into_iter()
        .find(|id| *id != old_id)
        .expect("the adopted replacement is in the registry");
    let entry = h2.entry(new_id);
    assert_eq!(entry.state, oceanfs_storage::segment::lifecycle::SegmentState::Sealed);
    assert_eq!(entry.repacked_from, None, "adoption seals without the marker");
    assert_eq!(entry.metadata.merkle_root, root_fn(&old_data), "the recomputed root matches");
    assert!(actions.is_empty(), "no marked unit → no compaction actions");
}

// ---------------------------------------------------------------------------
// Rows 7–9
// ---------------------------------------------------------------------------

/// Row 7: kill between `NewSealed` and `ObjectsMoved` — the new segment
/// is sealed (marker set), objects still point at the old → the new
/// `.dat` is an orphan → `SweepNewOrphan(new)`.
#[tokio::test]
async fn row7_kill_between_new_sealed_and_objects_moved() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let old_data = vec![0xEE; 400];
    let h = Harness::boot(dir.path()).await;
    // Seed the old segment and compact in the SAME harness: the
    // coordinator's registry must hold the seed (boot does not fold).
    h.seed_sealed_old(old_id, &old_data).await;
    h.put_object(
        "a.txt",
        ChunkRef {
            segment_id: old_id,
            offset: 0,
            length: 400,
            compressed: false,
            logical_length: 400,
        },
    );
    let old_meta = h.entry(old_id).metadata.clone();
    let handle = h.drive_to_milestone(old_id, old_meta, 3).await;
    h.kill(handle).await;
    drop(h);

    let h2 = Harness::boot(dir.path()).await;
    let (outcome, actions) = h2.recover().await;

    // Folded: new sealed with the marker; objects→old.
    let mut marked: Vec<(SegmentId, SegmentId)> = Vec::new();
    h2.lifecycle.registry().for_each(|id, entry| {
        if let Some(old) = entry.repacked_from {
            marked.push((id, old));
        }
    });
    let new_id = marked
        .into_iter()
        .find_map(|(id, old)| (old == old_id).then_some(id))
        .expect("the marked replacement is in the registry");
    let entry = h2.entry(new_id);
    assert_eq!(entry.state, oceanfs_storage::segment::lifecycle::SegmentState::Sealed);
    assert_eq!(entry.repacked_from, Some(old_id));
    assert_eq!(outcome.folded_segments, 2);
    assert_eq!(h2.object_chunk("a.txt").segment_id, old_id, "objects point at the old segment");
    assert!(h2.segments_dir.join(format!("{new_id}.dat")).exists(), "the new .dat is an orphan");
    assert_eq!(
        actions,
        vec![CompactionRecoveryAction::SweepNewOrphan(new_id)],
        "row 7: new .dat orphan → reaper"
    );

    // The action dispatched through the coordinator + shard store ends
    // the unit: the replacement is deleted durably and its .dat swept;
    // reads still resolve to the old segment.
    h2.lifecycle.request_delete(new_id).await.unwrap();
    h2.shard_store.delete_shards(new_id).unwrap();
    assert!(h2.lifecycle.registry().get(new_id).is_none(), "orphan deleted durably");
    assert!(
        !h2.segments_dir.join(format!("{new_id}.dat")).exists(),
        "orphan .dat swept after the durable delete"
    );
    assert_eq!(h2.object_chunk("a.txt").segment_id, old_id);
    assert_eq!(h2.entry(old_id).state, oceanfs_storage::segment::lifecycle::SegmentState::Sealed);
}

/// Row 8: kill between `ObjectsMoved` and `OldDeleted` — objects point
/// at the new segment, the old segment is still sealed → the old
/// sealed-orphan is finished: `FinishOldDeletion(old)`.
#[tokio::test]
async fn row8_kill_between_objects_moved_and_old_deleted() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let old_data = vec![0xEE; 400];
    let h = Harness::boot(dir.path()).await;
    // Seed the old segment and compact in the SAME harness: the
    // coordinator's registry must hold the seed (boot does not fold).
    h.seed_sealed_old(old_id, &old_data).await;
    h.put_object(
        "a.txt",
        ChunkRef {
            segment_id: old_id,
            offset: 0,
            length: 400,
            compressed: false,
            logical_length: 400,
        },
    );
    let old_meta = h.entry(old_id).metadata.clone();
    let handle = h.drive_to_milestone(old_id, old_meta, 3).await;
    h.advance_to(4).await;
    h.kill(handle).await;
    drop(h);

    let h2 = Harness::boot(dir.path()).await;
    let (outcome, actions) = h2.recover().await;

    // Folded: objects→new; old still sealed.
    let new_id = h2.object_chunk("a.txt").segment_id;
    assert_ne!(new_id, old_id, "objects point at the new segment");
    let entry = h2.entry(new_id);
    assert_eq!(entry.repacked_from, Some(old_id), "the marker identifies the unit");
    assert_eq!(h2.entry(old_id).state, oceanfs_storage::segment::lifecycle::SegmentState::Sealed);
    assert_eq!(outcome.folded_segments, 2);
    assert_eq!(
        actions,
        vec![CompactionRecoveryAction::FinishOldDeletion(old_id)],
        "row 8: old sealed-orphan → reaper request_delete"
    );

    // Dispatch: the old segment's durable deletion, then its .dat sweep.
    h2.lifecycle.request_delete(old_id).await.unwrap();
    h2.shard_store.delete_shards(old_id).unwrap();
    assert!(h2.lifecycle.registry().get(old_id).is_none(), "old deleted durably");
    assert!(!h2.segments_dir.join(format!("{old_id}.dat")).exists(), "old .dat swept");
    assert_eq!(h2.object_chunk("a.txt").segment_id, new_id, "reads resolve to the new segment");
}

/// Row 9: kill between `OldDeleted` and `OldRemoved` — the old segment
/// is deleted (its `DeleteEvent` is durable, entry evicted), only its
/// `.dat` residue remains → `SweepOldDat(old)`.
#[tokio::test]
async fn row9_kill_between_old_deleted_and_old_removed() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let old_data = vec![0xEE; 400];
    let h = Harness::boot(dir.path()).await;
    // Seed the old segment and compact in the SAME harness: the
    // coordinator's registry must hold the seed (boot does not fold).
    h.seed_sealed_old(old_id, &old_data).await;
    h.put_object(
        "a.txt",
        ChunkRef {
            segment_id: old_id,
            offset: 0,
            length: 400,
            compressed: false,
            logical_length: 400,
        },
    );
    let old_meta = h.entry(old_id).metadata.clone();
    let handle = h.drive_to_milestone(old_id, old_meta, 3).await;
    h.advance_to(4).await;
    h.advance_to(5).await;
    h.kill(handle).await;
    drop(h);

    let h2 = Harness::boot(dir.path()).await;
    let (outcome, actions) = h2.recover().await;

    // Folded: objects→new; old deleted (evicted with grace 0).
    let new_id = h2.object_chunk("a.txt").segment_id;
    assert_ne!(new_id, old_id);
    assert!(h2.lifecycle.registry().get(old_id).is_none(), "old deleted + evicted");
    // The fold established both histories; only the new segment is live.
    assert_eq!(outcome.folded_segments, 2);
    assert!(h2.segments_dir.join(format!("{old_id}.dat")).exists(), "old .dat residue survives");
    assert_eq!(
        actions,
        vec![CompactionRecoveryAction::SweepOldDat(old_id)],
        "row 9: old .dat orphan → sweep"
    );

    // Dispatch: the idempotent sweep.
    h2.shard_store.delete_shards(old_id).unwrap();
    assert!(!h2.segments_dir.join(format!("{old_id}.dat")).exists(), "old .dat swept");
    assert_eq!(h2.object_chunk("a.txt").segment_id, new_id, "reads resolve to the new segment");
}

/// The fully-dead path: kill after the `DeleteEvent(old)` — the old
/// segment is deleted, its `.dat` remains. No `repacked_from` marker
/// exists (no repack happened), so the recovery returns no action; the
/// residue is swept by the row-9 sweep mechanism (the startup `.dat`
/// sweep — `startup-rebuild-from-machine`).
#[tokio::test]
async fn fully_dead_kill_after_delete_leaves_dat_residue() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let old_data = vec![0xEE; 400];
    let h = Harness::boot(dir.path()).await;
    // Seed the old segment and compact in the SAME harness (boot does
    // not fold; the coordinator's registry must hold the seed).
    h.seed_sealed_old(old_id, &old_data).await;
    let old_meta = h.entry(old_id).metadata.clone();
    let handle = h.drive_to_milestone(old_id, old_meta, 5).await;
    h.kill(handle).await;
    drop(h);

    let h2 = Harness::boot(dir.path()).await;
    let (outcome, actions) = h2.recover().await;

    // Folded: old deleted (evicted), no objects referenced it at all.
    assert!(h2.lifecycle.registry().get(old_id).is_none(), "old deleted + evicted");
    // The fold established the old segment's history (reserve→seal→delete).
    assert_eq!(outcome.folded_segments, 1);
    assert!(h2.segments_dir.join(format!("{old_id}.dat")).exists(), ".dat residue survives");
    assert!(
        actions.is_empty(),
        "no repack happened → no marked unit → the .dat sweep covers the residue"
    );
}

// ---------------------------------------------------------------------------
// BadDigest regression — repack preserves the compression contract
// ---------------------------------------------------------------------------

/// The BadDigest mutation check: a compressed object survives a full
/// compaction + restart, and reading its chunk back through the new
/// segment's data yields the original logical bytes with a matching
/// digest. The repack must preserve `compressed` + `logical_length` +
/// the bytes verbatim (hardcoding `compressed: false` — the original
/// defect — flips the flag and fails this test).
#[tokio::test]
async fn repacked_compressed_chunk_reads_back_with_matching_digest() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // "Compression": the on-disk bytes are a transformed form of the
    // logical bytes (XOR 0xFF). The read path decompresses when
    // `compressed` is true, yielding `logical_length` bytes.
    let logical: Vec<u8> = (0..512).map(|i| (i * 7) as u8).collect();
    let on_disk: Vec<u8> = logical.iter().map(|b| b ^ 0xFF).collect();
    let digest_before = blake3::hash(&logical);

    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let h = Harness::boot(dir.path()).await;
    // Seed the old segment and compact in the SAME harness (boot does
    // not fold; the coordinator's registry must hold the seed).
    h.seed_sealed_old(old_id, &on_disk).await;
    h.put_object(
        "compressed.txt",
        ChunkRef {
            segment_id: old_id,
            offset: 0,
            length: on_disk.len() as u32,
            compressed: true,
            logical_length: logical.len() as u32,
        },
    );
    let old_meta = h.entry(old_id).metadata.clone();
    // Full compaction (no kill).
    h.compactor.compact_segment(old_id, &old_meta, &HashSet::new()).await.unwrap();
    drop(h);

    // Restart: the fold + data-WAL pass + recovery.
    let h2 = Harness::boot(dir.path()).await;
    let (outcome, _actions) = h2.recover().await;
    // The fold established both histories (old: reserve→seal→delete;
    // new: reserve→seal); only the new segment is live.
    assert_eq!(outcome.folded_segments, 2);
    assert!(h2.lifecycle.registry().get(old_id).is_none(), "the old segment is gone");

    // Read the object's chunk back from the NEW segment's data: the
    // repacked bytes must be the compressed form, the flags must be
    // preserved verbatim, and the inverse transform must reproduce the
    // logical bytes with a matching digest.
    let chunk = h2.object_chunk("compressed.txt");
    assert_ne!(chunk.segment_id, old_id, "the chunk was repacked");
    assert!(
        chunk.compressed,
        "the compression flag survives the repack (BadDigest mutation check)"
    );
    assert_eq!(chunk.logical_length, logical.len() as u32, "logical_length preserved");
    assert_eq!(chunk.length, on_disk.len() as u32, "the compressed size is preserved");

    let new_data = h2.data_store.read_segment_data(&chunk.segment_id).unwrap();
    let start = chunk.offset as usize;
    let end = start + chunk.length as usize;
    assert!(end <= new_data.len(), "chunk fits in the new segment's data section");
    // The AE anchor: the machine's Sealed entry carries the repacked
    // seal-time root — scrub/AE read the machine's root (ADR-0025
    // Decision 3; there is no CF mirror anymore).
    let entry = h2.entry(chunk.segment_id);
    assert_eq!(
        entry.metadata.merkle_root,
        root_fn(new_data.as_ref()),
        "the machine's Sealed entry carries the seal-time root"
    );
    let stored = &new_data[start..end];
    assert_eq!(stored, on_disk.as_slice(), "the compressed bytes are copied verbatim");
    let recovered: Vec<u8> = stored.iter().map(|b| b ^ 0xFF).collect();
    assert_eq!(recovered, logical, "the logical bytes are recoverable");
    assert_eq!(
        blake3::hash(&recovered).as_bytes(),
        digest_before.as_bytes(),
        "digest matches after compaction + restart (the BadDigest regression)"
    );
}

// ---------------------------------------------------------------------------
// Integration chain: compaction → restart → scrub → read-back
// ---------------------------------------------------------------------------

/// The scrub step of the DoD's integration chain: after a full
/// compaction + restart, the new segment scrubs healthy when verified
/// against the MACHINE's `Sealed` entry root (ADR-0025 Decision 3 —
/// scrub's root comes from the machine, not a CF), and the object
/// reads back with a matching digest.
#[tokio::test]
async fn post_compaction_segment_scrubs_healthy_against_the_machine_root() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let old_data = vec![0xEE; 400];
    let h = Harness::boot(dir.path()).await;
    h.seed_sealed_old(old_id, &old_data).await;
    h.put_object(
        "a.txt",
        ChunkRef {
            segment_id: old_id,
            offset: 0,
            length: 400,
            compressed: false,
            logical_length: 400,
        },
    );
    let old_meta = h.entry(old_id).metadata.clone();
    // Full compaction (no kill).
    h.compactor.compact_segment(old_id, &old_meta, &HashSet::new()).await.unwrap();
    drop(h);

    // Restart + recovery.
    let h2 = Harness::boot(dir.path()).await;
    h2.recover().await;

    // Scrub the post-compaction segment with the machine's root as the
    // expected anchor (the phase-2 mirror also carries it — asserted by
    // the BadDigest test).
    let new_id = h2.object_chunk("a.txt").segment_id;
    assert_ne!(new_id, old_id);
    let entry = h2.entry(new_id);
    assert_eq!(entry.state, oceanfs_storage::segment::lifecycle::SegmentState::Sealed);
    let scrubber =
        crate::scrub::ScrubWorker::new(Arc::clone(&h2.registry), h2.data_store.clone(), 0);
    let result = scrubber.scrub_segment(&entry.metadata);
    assert!(
        result.healthy && !result.merkle_mismatch && !result.skipped,
        "the repacked segment scrubs healthy against the machine's root: {result:?}"
    );
    assert!(result.bytes_scanned > 0);

    // Read-back resolves through the new segment with a matching digest.
    let chunk = h2.object_chunk("a.txt");
    let new_data = h2.data_store.read_segment_data(&chunk.segment_id).unwrap();
    let start = chunk.offset as usize;
    let end = start + chunk.length as usize;
    assert_eq!(&new_data[start..end], old_data.as_slice(), "uncompressed bytes verbatim");
    assert_eq!(
        blake3::hash(&new_data[start..end]).as_bytes(),
        blake3::hash(&old_data).as_bytes(),
        "digest matches after compaction + restart"
    );
}

/// The uncompressed half of the BadDigest regression: an uncompressed
/// object's chunk survives compaction + restart byte-identical with a
/// matching digest (the compressed variant is pinned by
/// `repacked_compressed_chunk_reads_back_with_matching_digest`).
#[tokio::test]
async fn repacked_uncompressed_chunk_reads_back_with_matching_digest() {
    let _seam = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let logical: Vec<u8> = (0..512).map(|i| (i * 3) as u8).collect();
    let digest_before = blake3::hash(&logical);

    let dir = tempfile::tempdir().unwrap();
    let old_id = SegmentId::new();
    let h = Harness::boot(dir.path()).await;
    h.seed_sealed_old(old_id, &logical).await;
    h.put_object(
        "plain.txt",
        ChunkRef {
            segment_id: old_id,
            offset: 0,
            length: logical.len() as u32,
            compressed: false,
            logical_length: logical.len() as u32,
        },
    );
    let old_meta = h.entry(old_id).metadata.clone();
    h.compactor.compact_segment(old_id, &old_meta, &HashSet::new()).await.unwrap();
    drop(h);

    // Restart: the fold + data-WAL pass + recovery.
    let h2 = Harness::boot(dir.path()).await;
    h2.recover().await;

    let chunk = h2.object_chunk("plain.txt");
    assert_ne!(chunk.segment_id, old_id, "the chunk was repacked");
    assert!(!chunk.compressed);
    assert_eq!(chunk.logical_length, logical.len() as u32);
    let new_data = h2.data_store.read_segment_data(&chunk.segment_id).unwrap();
    let start = chunk.offset as usize;
    let end = start + chunk.length as usize;
    let stored = &new_data[start..end];
    assert_eq!(stored, logical.as_slice(), "uncompressed bytes copied verbatim");
    assert_eq!(
        blake3::hash(stored).as_bytes(),
        digest_before.as_bytes(),
        "digest matches after compaction + restart"
    );
}
