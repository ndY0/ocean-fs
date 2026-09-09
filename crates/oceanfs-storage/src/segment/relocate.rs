//! The durable segment-relocation primitive (d2, ADR-0036 D2/D3).
//!
//! [`SegmentRelocator`] durably moves a sealed segment's `.dat` from one
//! data pool to another on the same node by mutating the segment's
//! `pool_id` through the existing metadata-refresh event family (the
//! event-WAL + checkpoint fold stays the only durable writer,
//! ADR-0025). Relocation is **copy → commit → unlink** under the unified
//! store's per-segment write lock (ADR-0032 D3):
//!
//! 1. **copy** — the source `.dat`'s data section is read and written to
//!    the **target pool root** through the store's atomic write path
//!    with an explicit target pool id (`resolve_pool` skipped);
//! 2. **commit** — a `MetadataRefresh` event carrying `pool_id =
//!    Some(target)` is appended (durable) and folded, flipping the
//!    registry entry's `pool_id`;
//! 3. **unlink** — the source-root `.dat` is removed with an
//!    explicit-source-pool unlink.
//!
//! Both crash windows are safe (ADR-0036 D3):
//! - *pre-commit* (copy durable, event not): the registry still points at
//!   the source, so the source is authoritative; the target copy is an
//!   unregistered `.dat` on the target root, reaped as residue by the
//!   boot sweep (the d2 root-vs-`pool_id` mismatch rule) or overwritten
//!   idempotently by a re-run;
//! - *post-commit* (event durable, unlink not): the registry points at
//!   the target, so the target is authoritative; the surviving source
//!   `.dat` is residue on the old root, reaped at boot.
//!
//! Reads resolve by registry `pool_id`, so the source→target switch is
//! atomic at the commit even while two `.dat` copies exist; the store's
//! per-segment write lock guarantees no concurrent writer interleaves.
//! GC/compaction/AE/scrub/orphan-reaper all resolve from the registry, so
//! after the commit they see the new `pool_id` and never touch the stale
//! source copy.
//!
//! The mover workers are d3 (intra-node) and d4 (cluster): they call
//! [`SegmentRelocator::relocate`] per registry-enumerated segment — this
//! primitive never scans the disk itself (ADR-0034 bounded-metadata
//! discipline).

use std::sync::Arc;

use oceanfs_core::{PoolRole, SegmentId};
use oceanfs_storage_api::SegmentDataStore;

use crate::segment::{
    data_store::{DiskSegmentStore, SegmentWriteGuard},
    lifecycle::{SegmentLifecycleCoordinator, SegmentState},
};

/// Relocation errors — a deterministic taxonomy the d3/d4 drain workers
/// use to block with a precise reason (no-destructive-failure rule).
///
/// # Examples
///
/// ```
/// use oceanfs_storage::RelocateError;
///
/// let err = RelocateError::TargetPoolMissing(9);
/// assert_eq!(err.to_string(), "relocation target pool 9 is not registered");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RelocateError {
    /// No registered pool carries the target id.
    #[error("relocation target pool {0} is not registered")]
    TargetPoolMissing(u32),
    /// The target pool exists but is not a `data`-role pool (only data
    /// pools hold segment `.dat` files; wal/metadata/hints cannot).
    #[error("relocation target pool {0} is not a data pool")]
    TargetNotDataRole(u32),
    /// The target pool equals the segment's current pool — nothing to
    /// move.
    #[error("segment {segment_id} already lives on pool {pool_id}")]
    SamePool {
        /// The segment that is already on the requested pool.
        segment_id: SegmentId,
        /// Its current pool (== the requested target).
        pool_id: u32,
    },
    /// The segment is not `Sealed` (only sealed segments have a durable
    /// `.dat` a relocation may move).
    #[error("segment {0} is not sealed")]
    NotSealed(SegmentId),
    /// The segment has no local lifecycle entry (this node holds no
    /// copy to relocate).
    #[error("segment {0} is not held locally")]
    NotHeldLocally(SegmentId),
    /// The registry says the segment is sealed but the source `.dat` is
    /// gone — never relocate a ghost.
    #[error("segment {0} source .dat is missing from its pool root")]
    SourceFileMissing(SegmentId),
    /// The durable commit (event-WAL append + fold) failed.
    #[error("relocation commit failed for segment {segment_id}: {detail}")]
    Commit {
        /// The segment whose commit failed.
        segment_id: SegmentId,
        /// The underlying transition/durability error text.
        detail: String,
    },
    /// A store I/O step failed (copy or unlink).
    #[error("relocation store operation failed for segment {0}: {1}")]
    Store(SegmentId, String),
}

/// Relocates a sealed segment's `.dat` between data pools on one node.
///
/// Orchestrates the lifecycle coordinator (the event-WAL writer that
/// commits the `pool_id` mutation) and the unified [`DiskSegmentStore`]
/// (the per-segment write lock + atomic file operations). Constructed by
/// the d3/d4 composition root from the same `Arc`s the node already
/// holds.
///
/// # Examples
///
/// ```ignore
/// // Requires a fully wired coordinator (with an attached event-WAL) and
/// // a concrete DiskSegmentStore over the live pool registry — see the
/// // storage-module wiring and the relocate unit tests.
/// let relocator = SegmentRelocator::new(lifecycle_coordinator, data_store);
/// relocator.relocate(segment_id, 1).await?;
/// ```
#[derive(Clone)]
pub struct SegmentRelocator {
    coordinator: Arc<SegmentLifecycleCoordinator>,
    store: Arc<DiskSegmentStore>,
}

impl SegmentRelocator {
    /// Creates the relocator over the node's lifecycle coordinator and
    /// unified data store.
    pub fn new(
        coordinator: Arc<SegmentLifecycleCoordinator>,
        store: Arc<DiskSegmentStore>,
    ) -> Self {
        Self { coordinator, store }
    }

    /// Durably moves a sealed segment from its current data pool to
    /// `target_pool_id` (copy → commit → unlink, ADR-0036 D3).
    ///
    /// Runs entirely under the segment's per-segment write lock, so no
    /// concurrent whole-file writer can interleave. On success the
    /// registry's `pool_id` is the target, the `.dat` exists only on the
    /// target root, and the reader's per-segment root cache has been
    /// purged (the next read re-resolves the target root).
    ///
    /// # Errors
    ///
    /// All [`RelocateError`] variants: target validation, segment state,
    /// a missing source file (never relocate a ghost), or a durable
    /// commit / store failure. A failure leaves the segment fully on its
    /// source pool (no partial state — the target copy is atomically
    /// written and unregistered until the commit).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Full-wiring example; see the relocate unit tests for a
    /// // 2-data-pool harness.
    /// relocator.relocate(segment_id, 1).await?;
    /// ```
    pub async fn relocate(
        &self,
        segment_id: SegmentId,
        target_pool_id: u32,
    ) -> Result<(), RelocateError> {
        // ---- Validate the target pool (registered, data role). ----
        let target_pool = self
            .store
            .pool_registry()
            .pool_by_id(target_pool_id)
            .ok_or(RelocateError::TargetPoolMissing(target_pool_id))?;
        if target_pool.role() != PoolRole::Data {
            return Err(RelocateError::TargetNotDataRole(target_pool_id));
        }

        // ---- Validate the segment (locally held, Sealed). ----
        let entry = self
            .store
            .lifecycle_registry()
            .get(segment_id)
            .ok_or(RelocateError::NotHeldLocally(segment_id))?;
        if entry.state != SegmentState::Sealed {
            return Err(RelocateError::NotSealed(segment_id));
        }
        let source_pool_id = entry.metadata.pool_id;
        if source_pool_id == target_pool_id {
            return Err(RelocateError::SamePool { segment_id, pool_id: source_pool_id });
        }

        // ---- Per-segment exclusive lock across the whole sequence. ----
        let guard: SegmentWriteGuard = self.store.lock_segment(&segment_id).await;

        // 1. copy: read the source data section; write it to the target
        // pool root via the explicit-target atomic write (resolve_pool
        // skipped). A missing source file is a distinct error — the
        // registry says we hold it, so never "relocate" a ghost.
        let source = self
            .store
            .read_segment_data(&segment_id)
            .await
            .map_err(|e| RelocateError::Store(segment_id, e.to_string()))?
            .ok_or(RelocateError::SourceFileMissing(segment_id))?;
        self.store
            .write_segment_data_to_pool_guarded(&segment_id, target_pool_id, &source.data, &guard)
            .await
            .map_err(|e| RelocateError::Store(segment_id, e.to_string()))?;

        // 2. commit: durable MetadataRefresh { pool_id = Some(target) }
        // (event-WAL append + fold — the only durable writer). The
        // registry switch is atomic for all readers from here on. The
        // seal-time merkle anchor is carried explicitly — the refresh's
        // merkle parameter is a value replacement (None clears it), and a
        // relocation must never lose the anchor fetch verification trusts.
        self.coordinator
            .request_refresh_metadata(
                segment_id,
                entry.metadata.merkle_root,
                None,
                Some(target_pool_id),
            )
            .await
            .map_err(|e| RelocateError::Commit { segment_id, detail: e.to_string() })?;

        // Purge the shared reader's root cache AFTER the commit: a read
        // that cached the source root between the copy and the commit
        // must re-resolve the target on its next request.
        self.store.purge_reader_cache(&segment_id);

        // 3. unlink: explicit-source-pool removal — registry-independent,
        // so it removes exactly the source-root `.dat`.
        self.store
            .delete_shards_with_pool(&segment_id, source_pool_id)
            .await
            .map_err(|e| RelocateError::Store(segment_id, e.to_string()))?;

        Ok(())
    }
}

/// Classifies one `.dat` found on a data-pool root during the once-per-
/// boot residue sweep (startup only).
///
/// `true` = unlink (residue); `false` = keep (authoritative). Rules:
/// - no live lifecycle entry → residue (the durable delete already
///   folded; the unlink was pending);
/// - `Deleted` entry → residue (delete durable, unlink pending);
/// - `Sealed` entry whose **authoritative pool** (`metadata.pool_id`)
///   differs from the root the file was found on → residue — the d2
///   relocation crash windows (ADR-0036 D3) leave exactly one such copy:
///   the pre-commit target file (registry still on the source pool) or
///   the post-commit source file (registry already on the target pool);
/// - `Reserved` → keep: a reserved segment's `.dat` belongs to the
///   data-WAL row-3 adoption path (recompute root → SealEvent), which
///   owns unsealed files — this sweep must not step on it.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::{is_startup_residue, SegmentState};
///
/// // No live entry → residue.
/// assert!(is_startup_residue(None, 0, 1));
/// // Deleted → residue.
/// assert!(is_startup_residue(Some(SegmentState::Deleted), 0, 0));
/// // Sealed copy on the wrong root (post-commit source leftover) →
/// // residue.
/// assert!(is_startup_residue(Some(SegmentState::Sealed), 1, 0));
/// // The authoritative copy is kept.
/// assert!(!is_startup_residue(Some(SegmentState::Sealed), 1, 1));
/// // Reserved files are left to the row-3 adoption path.
/// assert!(!is_startup_residue(Some(SegmentState::Reserved), 0, 1));
/// ```
pub fn is_startup_residue(
    state: Option<SegmentState>,
    authoritative_pool_id: u32,
    found_pool_id: u32,
) -> bool {
    match state {
        None => true,
        Some(SegmentState::Deleted) => true,
        Some(SegmentState::Sealed) => authoritative_pool_id != found_pool_id,
        Some(SegmentState::Reserved) => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use bytes::Bytes;
    use oceanfs_core::{LifecycleConfig, PoolRole, SegmentId, SizeTier, StorageConfig};
    use parking_lot::Mutex;

    use super::*;
    use crate::{
        io::{IoBackend, IoObserver, IoReadMode, SegmentReader},
        pool::PoolRegistry,
        segment::lifecycle::SegmentLifecycleRegistry,
    };

    /// A reader that records `purge_cache` calls (proves the relocation
    /// commit purges the reader's per-segment root cache).
    struct TrackingReader {
        purged: Arc<Mutex<Vec<SegmentId>>>,
    }

    #[async_trait::async_trait]
    impl SegmentReader for TrackingReader {
        async fn read_chunk(
            &self,
            _segment_id: &SegmentId,
            _offset: u64,
            _length: u32,
        ) -> std::result::Result<Bytes, String> {
            Err("the relocation test env never reads chunks through the injected reader".into())
        }

        fn purge_cache(&self, segment_id: &SegmentId) {
            self.purged.lock().push(*segment_id);
        }
    }

    /// A pools-only store over **two** data pools (config-order ids 0, 1)
    /// plus the mandatory wal/metadata/hints siblings, with a real
    /// event-WAL + coordinator + tracking reader. Built over a caller-
    /// held tempdir so a crash test can rebuild a second env over the
    /// same directories.
    struct RelocateEnv {
        store: Arc<DiskSegmentStore>,
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        event_wal: Arc<crate::segment::event_wal::EventWal>,
        roots: Vec<PathBuf>,
        purged: Arc<Mutex<Vec<SegmentId>>>,
    }

    fn pool_config(
        name: &str,
        role: PoolRole,
        root: &std::path::Path,
    ) -> oceanfs_core::StoragePoolConfig {
        oceanfs_core::StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight: Some(1),
            tech: oceanfs_core::PoolTech::Auto,
            health: Default::default(),
        }
    }

    async fn make_env() -> (tempfile::TempDir, RelocateEnv) {
        let tmp = tempfile::tempdir().unwrap();
        let env = build_env(&tmp).await;
        (tmp, env)
    }

    /// Builds the 2-pool relocation env under an existing tempdir (the
    /// dirs are derived from `tmp`, so a second env over the same dirs
    /// simulates a cold restart).
    async fn build_env(tmp: &tempfile::TempDir) -> RelocateEnv {
        let roots = vec![tmp.path().join("pool-data-0"), tmp.path().join("pool-data-1")];
        let storage = StorageConfig {
            pools: vec![
                pool_config("data-0", PoolRole::Data, &roots[0]),
                pool_config("data-1", PoolRole::Data, &roots[1]),
                pool_config("wal-0", PoolRole::Wal, &tmp.path().join("pool-wal")),
                pool_config("meta-0", PoolRole::Metadata, &tmp.path().join("pool-meta")),
                pool_config("hints-0", PoolRole::Hints, &tmp.path().join("pool-hints")),
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        };
        let pool_registry =
            Arc::new(PoolRegistry::from_config(&storage, &tmp.path().join("meta")).unwrap());
        let lifecycle_registry =
            Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let event_wal_config = oceanfs_core::EventWalConfig {
            event_wal_dir: tmp.path().join("event-wal"),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024 * 1024,
        };
        let event_wal = Arc::new(
            crate::segment::event_wal::EventWal::open(
                tmp.path().join("event-wal"),
                &event_wal_config,
            )
            .await
            .unwrap(),
        );
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&lifecycle_registry))
                .with_event_wal(event_wal.clone()),
        );
        let observer = Arc::new(IoObserver::new());
        observer.register_pool(0, None);
        observer.register_pool(1, None);
        let purged = Arc::new(Mutex::new(Vec::new()));
        let reader: Arc<dyn SegmentReader> =
            Arc::new(TrackingReader { purged: Arc::clone(&purged) });
        let store = Arc::new(DiskSegmentStore::new(
            Arc::clone(&pool_registry),
            Arc::clone(&lifecycle_registry),
            reader,
            IoReadMode::Buffered,
            Arc::new(IoBackend::default()),
            observer,
        ));
        RelocateEnv { store, lifecycle, event_wal, roots, purged }
    }

    /// Cold-restarts the node over the same directories: reopens the
    /// event-WAL, builds a fresh registry + coordinator + store, and
    /// replays the full event log (the boot fold). The fresh env's store
    /// resolves the `.dat` files by the replayed `pool_id`.
    async fn cold_restart(tmp: &tempfile::TempDir) -> RelocateEnv {
        let env = build_env(tmp).await;
        env.lifecycle
            .rebuild_from_events(
                env.event_wal
                    .read_from(crate::segment::event_wal::EventWalPos { file_seq: 0, offset: 0 }),
            )
            .expect("boot fold replays every durable event");
        env
    }

    /// The once-per-boot residue sweep, exactly as `modules/storage.rs`
    /// runs it: list each data pool root and unlink every `.dat`
    /// [`is_startup_residue`] classifies as residue.
    async fn run_boot_sweep(env: &RelocateEnv) {
        for (pool_id, root) in env.roots.iter().enumerate() {
            for path in env.store.list_segment_files(root).unwrap() {
                let Some(id_str) =
                    path.file_name().and_then(|n| n.to_str()).and_then(|n| n.strip_suffix(".dat"))
                else {
                    continue;
                };
                let Ok(uuid) = uuid::Uuid::parse_str(id_str) else { continue };
                let segment_id = oceanfs_core::SegmentId::from_uuid_bytes(*uuid.as_bytes());
                let entry = env.store.lifecycle_registry().get(segment_id);
                let residue = is_startup_residue(
                    entry.as_ref().map(|e| e.state),
                    entry.as_ref().map(|e| e.metadata.pool_id).unwrap_or(0),
                    pool_id as u32,
                );
                if residue {
                    env.store.delete_shards_with_pool(&segment_id, pool_id as u32).await.unwrap();
                }
            }
        }
    }

    /// Seeds a sealed segment whose `.dat` lives on pool `pool_id`'s root
    /// and holds `data`.
    async fn seed_on(env: &RelocateEnv, pool_id: u32, data: &[u8]) -> SegmentId {
        let id = SegmentId::new();
        env.lifecycle.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        let merkle_root = oceanfs_core::HashOutput::from_bytes(*blake3::hash(data).as_bytes());
        let meta = oceanfs_core::SegmentMetadata {
            pool_id,
            total_bytes: data.len() as u64,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1_700_000_000_000),
        };
        env.lifecycle.request_seal(id, meta, None).await.unwrap();
        env.store.write_segment_data(&id, data).await.unwrap();
        assert!(env.roots[pool_id as usize].join(format!("{id}.dat")).exists());
        id
    }

    fn relocator(env: &RelocateEnv) -> SegmentRelocator {
        SegmentRelocator::new(Arc::clone(&env.lifecycle), Arc::clone(&env.store))
    }

    #[tokio::test]
    async fn relocate_moves_file_flips_pool_id_and_purges_reader() {
        let (_tmp, env) = make_env().await;
        let data: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let id = seed_on(&env, 0, &data).await;

        relocator(&env).relocate(id, 1).await.expect("relocate pool 0 → pool 1");

        // The file moved roots.
        assert!(!env.roots[0].join(format!("{id}.dat")).exists(), "source copy unlinked");
        assert!(env.roots[1].join(format!("{id}.dat")).exists(), "target copy present");

        // The registry's pool_id flipped (registry-driven resolution).
        let entry = env.store.lifecycle_registry().get(id).expect("entry");
        assert_eq!(entry.metadata.pool_id, 1);
        // A pool_id-only refresh must not clear the seal-time merkle root
        // (fetch verification on the re-replication path trusts it).
        assert!(entry.metadata.merkle_root.is_some());

        // The read path resolves the new root and returns identical bytes.
        let file = env.store.read_segment_data(&id).await.unwrap().expect("readable");
        assert_eq!(&file.data[..], &data[..], "byte-identical content after the switch");

        // The reader's per-segment root cache was purged at the commit.
        assert!(
            env.purged.lock().contains(&id),
            "relocate must purge the reader's per-segment cache"
        );
    }

    #[tokio::test]
    async fn relocate_rejects_invalid_targets() {
        let (_tmp, env) = make_env().await;
        let data = vec![9u8; 128];
        let id = seed_on(&env, 0, &data).await;
        let relocator = relocator(&env);

        // Unknown target pool.
        assert_eq!(relocator.relocate(id, 99).await, Err(RelocateError::TargetPoolMissing(99)));
        // wal/metadata/hints role target (id 2 = the wal pool here).
        assert_eq!(relocator.relocate(id, 2).await, Err(RelocateError::TargetNotDataRole(2)));
        // Same pool.
        assert_eq!(
            relocator.relocate(id, 0).await,
            Err(RelocateError::SamePool { segment_id: id, pool_id: 0 })
        );
        // Unknown segment (not held locally).
        let unknown = SegmentId::new();
        assert_eq!(
            relocator.relocate(unknown, 1).await,
            Err(RelocateError::NotHeldLocally(unknown)),
        );
    }

    #[tokio::test]
    async fn relocate_rejects_not_sealed_and_source_missing() {
        let (_tmp, env) = make_env().await;

        // Reserved-but-not-sealed → NotSealed.
        let reserved = SegmentId::new();
        env.lifecycle.request_reserve(reserved, SizeTier::Standard, 4, 2).await.unwrap();
        assert_eq!(
            relocator(&env).relocate(reserved, 1).await,
            Err(RelocateError::NotSealed(reserved))
        );

        // Sealed-but-never-written → SourceFileMissing (never relocate a ghost).
        let ghost = SegmentId::new();
        env.lifecycle.request_reserve(ghost, SizeTier::Standard, 4, 2).await.unwrap();
        let meta = oceanfs_core::SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: ghost,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([7; 32])),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1_700_000_000_000),
        };
        env.lifecycle.request_seal(ghost, meta, None).await.unwrap();
        assert_eq!(
            relocator(&env).relocate(ghost, 1).await,
            Err(RelocateError::SourceFileMissing(ghost))
        );
    }

    /// A relocation serializes against a concurrent plain writer on the
    /// same segment (per-segment write lock): whichever order the tasks
    /// interleave, exactly one complete payload survives and the
    /// registry/pool_id settle to the target.
    #[tokio::test]
    async fn relocate_serializes_against_concurrent_plain_writer() {
        let (_tmp, env) = make_env().await;
        let data: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();
        let id = seed_on(&env, 0, &data).await;

        let store = Arc::clone(&env.store);
        let lifecycle = Arc::clone(&env.lifecycle);
        let mover = Arc::new(SegmentRelocator::new(lifecycle, store));

        let mut handles = Vec::new();
        for round in 0..4u8 {
            let mover = Arc::clone(&mover);
            let store = Arc::clone(&env.store);
            handles.push(tokio::spawn(async move {
                let payload = vec![round; 1024];
                let _ = store.write_segment_data(&id, &payload).await;
            }));
            handles.push(tokio::spawn(async move {
                let _ = mover.relocate(id, 1).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Settled state: the registry points at pool 1 and exactly one
        // complete `.dat` exists there (never interleaved bytes).
        let entry = env.store.lifecycle_registry().get(id).expect("entry");
        assert_eq!(entry.metadata.pool_id, 1);
        assert!(!env.roots[0].join(format!("{id}.dat")).exists());
        let path = env.roots[1].join(format!("{id}.dat"));
        assert!(path.exists(), "a settled target copy exists");
        let raw = std::fs::read(&path).unwrap();
        assert!(raw.len() > 76);
        let data_section = &raw[76..];
        assert!(
            (0..4u8).any(|round| data_section == vec![round; 1024].as_slice())
                || data_section == data.as_slice(),
            "the settled .dat must equal exactly one complete payload"
        );
    }

    /// The durable holder-set stamp (persist_storage_locations) must
    /// survive a cold restart: a node's knowledge that it holds a segment
    /// is folded back from the event WAL (d4 re-issue / g4 live-count).
    #[tokio::test]
    async fn storage_locations_stamp_survives_cold_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let locations_set = {
            let env = build_env(&tmp).await;
            let data = vec![21u8; 256];
            let id = seed_on(&env, 0, &data).await;
            let mut locations = smallvec::SmallVec::<[oceanfs_core::NodeId; 16]>::new();
            locations.push(oceanfs_core::NodeId::new("node-b"));
            locations.push(oceanfs_core::NodeId::new("node-c"));
            env.lifecycle.persist_storage_locations(id, locations.clone()).await.unwrap();
            // Give the fsync batch a beat before the simulated crash.
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            (id, locations)
        };
        let (id, locations) = locations_set;

        let env = cold_restart(&tmp).await;
        let entry = env.store.lifecycle_registry().get(id).expect("entry restored");
        assert_eq!(
            entry.metadata.storage_locations.to_vec(),
            locations.to_vec(),
            "the durable holder stamp is folded back after a cold restart"
        );
        assert!(
            entry.metadata.merkle_root.is_some(),
            "a location-only refresh must never clear the seal-time merkle anchor"
        );
    }

    /// ADR-0036 D3 pre-commit crash window, end-to-end: the copy landed
    /// on the target root but the refresh event was never committed.
    /// After a cold restart (reopen + replay fold) the registry stays on
    /// the source (authoritative) and the **real boot residue sweep**
    /// reaps the target copy — no data loss, exactly one authoritative
    /// `.dat`.
    #[tokio::test]
    async fn precommit_crash_restart_fold_keeps_source_and_boot_sweep_reaps_target() {
        let tmp = tempfile::tempdir().unwrap();
        let data = vec![11u8; 512];

        {
            let env = build_env(&tmp).await;
            let id = seed_on(&env, 0, &data).await;
            // Copy step only — the crash lands before the durable commit.
            let guard = env.store.lock_segment(&id).await;
            let source = env.store.read_segment_data(&id).await.unwrap().unwrap();
            env.store
                .write_segment_data_to_pool_guarded(&id, 1, &source.data, &guard)
                .await
                .unwrap();
            drop(guard);
            assert!(env.roots[1].join(format!("{id}.dat")).exists(), "target copy present");
            // Give the fsync batch a beat before the simulated crash.
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        } // "crash": drop every handle.

        // Cold restart: reopen + replay fold. No refresh event was
        // durable, so the registry lands on the source pool.
        let env = cold_restart(&tmp).await;
        let id = {
            // Re-discover the segment id from the surviving source file.
            let listed = env.store.list_segment_files(&env.roots[0]).unwrap();
            let name = listed[0].file_name().unwrap().to_string_lossy().into_owned();
            let id_str = name.strip_suffix(".dat").unwrap();
            let uuid = uuid::Uuid::parse_str(id_str).unwrap();
            oceanfs_core::SegmentId::from_uuid_bytes(*uuid.as_bytes())
        };
        assert_eq!(env.store.lifecycle_registry().get(id).unwrap().metadata.pool_id, 0);
        assert!(env.roots[1].join(format!("{id}.dat")).exists(), "residue present after restart");

        // Run the boot residue sweep: the target copy is reaped, the
        // source stays authoritative and serves identical bytes.
        run_boot_sweep(&env).await;
        assert!(!env.roots[1].join(format!("{id}.dat")).exists(), "target residue reaped");
        assert!(env.roots[0].join(format!("{id}.dat")).exists(), "source authoritative");
        let file = env.store.read_segment_data(&id).await.unwrap().expect("source serves");
        assert_eq!(&file.data[..], &data[..], "byte-identical after restart + sweep");
    }

    /// ADR-0036 D3 post-commit crash window, end-to-end: the refresh
    /// event committed durably (registry pool_id = target) but the source
    /// unlink never ran. A cold restart fold reproduces the target pool,
    /// and the real boot residue sweep reaps the surviving source copy —
    /// no data loss, exactly one authoritative `.dat`.
    #[tokio::test]
    async fn postcommit_crash_restart_fold_keeps_target_and_boot_sweep_reaps_source() {
        let tmp = tempfile::tempdir().unwrap();
        let data = vec![12u8; 512];

        {
            let env = build_env(&tmp).await;
            let id = seed_on(&env, 0, &data).await;
            // Copy + durable commit, then stop before the unlink.
            let guard = env.store.lock_segment(&id).await;
            let source = env.store.read_segment_data(&id).await.unwrap().unwrap();
            env.store
                .write_segment_data_to_pool_guarded(&id, 1, &source.data, &guard)
                .await
                .unwrap();
            drop(guard);
            env.lifecycle
                .request_refresh_metadata(id, None, None, Some(1))
                .await
                .expect("durable commit");
            assert!(env.roots[0].join(format!("{id}.dat")).exists(), "source copy not unlinked");
            // Give the fsync batch a beat before the simulated crash.
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        } // "crash": drop every handle.

        // Cold restart: the durable refresh event replays to the target
        // pool (the switch survives the restart).
        let env = cold_restart(&tmp).await;
        let id = {
            let listed = env.store.list_segment_files(&env.roots[1]).unwrap();
            let name = listed[0].file_name().unwrap().to_string_lossy().into_owned();
            let id_str = name.strip_suffix(".dat").unwrap();
            let uuid = uuid::Uuid::parse_str(id_str).unwrap();
            oceanfs_core::SegmentId::from_uuid_bytes(*uuid.as_bytes())
        };
        assert_eq!(env.store.lifecycle_registry().get(id).unwrap().metadata.pool_id, 1);
        assert!(env.roots[0].join(format!("{id}.dat")).exists(), "source residue present");

        // Run the boot residue sweep: the stale source copy is reaped,
        // the target stays authoritative and serves identical bytes.
        run_boot_sweep(&env).await;
        assert!(!env.roots[0].join(format!("{id}.dat")).exists(), "source residue reaped");
        assert!(env.roots[1].join(format!("{id}.dat")).exists(), "target authoritative");
        let file = env.store.read_segment_data(&id).await.unwrap().expect("target serves");
        assert_eq!(&file.data[..], &data[..], "byte-identical after restart + sweep");
    }
}
