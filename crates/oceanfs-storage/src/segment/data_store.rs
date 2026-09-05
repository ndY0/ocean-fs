//! The unified segment data store (ADR-0032 D2/D3).
//!
//! The ONE production implementation of
//! [`oceanfs_storage_api::SegmentDataStore`]: whole-file `.dat`
//! read/write/delete/list over the storage pool layout. It merges the
//! durability crate's deleted field-for-field duplicate impl pair and routes I/O
//! through the storage `io` layer — reads through the shared file core
//! (`crate::io::segment_file::SegmentFileReader`, the same
//! implementation the server chunk reader uses), writes through the
//! atomic temp-file discipline of the seal pipeline, recorded on the
//! pool's [`crate::io::IoObserver`].
//!
//! ## Invariants
//!
//! - **Pools only (ADR-0031):** no `legacy_dir`, no empty-pools branch.
//!   A segment's pool root resolves from the lifecycle registry's
//!   `pool_id` (ADR-0025) against the live [`PoolRegistry`].
//! - **Lifecycle-routed writes (ADR-0032 D3):** every `.dat` mutation is
//!   single-writer per segment — a per-segment exclusive lock makes
//!   concurrent writers to one `.dat` unrepresentable. Writers must
//!   `request_reserve` (or already hold a registered entry) BEFORE
//!   writing; there is no write-before-register pool-0 fallback.
//! - **Purge-on-write:** after a whole-file rewrite the shared
//!   reader's per-segment caches are purged so the server chunk path
//!   never serves stale header/size/mmap facts.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use oceanfs_core::SegmentId;
use oceanfs_storage_api::{
    error::{Error, Result},
    SegmentDataStore, SegmentFile,
};
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};

use crate::{
    io::{self, DiskIo, SegmentWriteMode},
    pool::PoolRegistry,
    segment::{
        header::{SEGMENT_HEADER_SIZE_V1, SEGMENT_MAGIC},
        lifecycle::SegmentLifecycleRegistry,
    },
};

/// A per-segment exclusive write guard (ADR-0032 D3).
///
/// Returned by [`DiskSegmentStore::lock_segment`]; holding it marks "I
/// own this `.dat` right now". Multi-step read-modify-write flows (EC
/// heal decode + splice) take the guard across the whole sequence so
/// two writers can never interleave; `write_segment_data` acquires the
/// same lock internally.
///
/// The guard is not reentrant: while it is held, the rewrite must go
/// through [`DiskSegmentStore::write_segment_data_guarded`] — calling
/// the plain `write_segment_data` would self-deadlock on the same
/// per-segment mutex.
///
/// # Examples
///
/// ```ignore
/// // Requires a fully wired store (pools + lifecycle registry + io).
/// let guard = store.lock_segment(&segment_id).await;
/// // ... read, decode, splice ...
/// store
///     .write_segment_data_guarded(&segment_id, &updated, &guard)
///     .await?;
/// drop(guard);
/// ```
#[derive(Debug)]
pub struct SegmentWriteGuard {
    _guard: OwnedMutexGuard<()>,
    /// The segment whose `.dat` this guard owns.
    segment_id: SegmentId,
}

impl SegmentWriteGuard {
    /// Returns the segment this guard owns.
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }
}

/// The unified segment data store (ADR-0032 D2).
///
/// Constructed once by the composition root's `StorageModule` (f3) and
/// shared by GC, AE, heal, scrub, re-replication, the replicator, and
/// the segment/healing gRPC services.
///
/// # Examples
///
/// ```ignore
/// // Construction requires a booted pool registry + lifecycle registry
/// // + io handles; see the storage-module wiring and unit tests.
/// let store = DiskSegmentStore::new(
///     registry,
///     lifecycle_registry,
///     reader,
///     io_mode,
///     io_backend,
///     observer,
/// );
/// ```
pub struct DiskSegmentStore {
    /// Live pool registry — root resolution for caller-held pool ids
    /// (ADR-0031: pools only; a runtime-attached pool resolves because
    /// the registry is live, not a boot snapshot).
    pools: Arc<PoolRegistry>,
    /// Lifecycle registry — the `pool_id` per segment (ADR-0025).
    lifecycle_registry: Arc<SegmentLifecycleRegistry>,
    /// The shared segment reader: after every whole-file rewrite its
    /// per-segment caches are purged (stale header/size/mmap facts must
    /// not outlive the file they describe).
    reader: Arc<dyn io::SegmentReader>,
    /// The shared path-agnostic read core — no mmap LRU: whole-file
    /// scans (scrub/AE/GC) never populate the bounded cache.
    file: io::segment_file::SegmentFileReader,
    /// The io backend the write path's observed ops delegate to.
    io_backend: Arc<io::IoBackend>,
    /// Per-pool signal accumulator (ADR-0029 §D3): every write/fsync is
    /// recorded on the pool whose root the `.dat` lands on.
    observer: Arc<io::IoObserver>,
    /// Atomic write mode probed lazily per pool root (pools can sit on
    /// different filesystems; `O_TMPFILE` support is per-fs).
    write_modes: ParkingMutex<HashMap<u32, SegmentWriteMode>>,
    /// Per-segment exclusive write locks (ADR-0032 D3): concurrent
    /// writers to one `.dat` are unrepresentable, not just discouraged.
    /// Entries are per-written-segment (segment ids are never reused) —
    /// the same growth model as the reader's per-segment caches.
    write_locks: ParkingMutex<HashMap<SegmentId, Arc<TokioMutex<()>>>>,
}

impl DiskSegmentStore {
    /// Creates the unified store.
    ///
    /// `io_mode` mirrors the seal pipeline's write mode (Direct = the
    /// O_DIRECT whole-file temp path; otherwise the probed atomic
    /// write mode per pool root). Reads dispatch per `io_mode` through
    /// the shared file core exactly like the server chunk reader.
    ///
    /// # Errors
    ///
    /// Construction never fails; per-operation errors surface on the
    /// trait methods.
    pub fn new(
        pools: Arc<PoolRegistry>,
        lifecycle_registry: Arc<SegmentLifecycleRegistry>,
        reader: Arc<dyn io::SegmentReader>,
        io_mode: io::IoReadMode,
        io_backend: Arc<io::IoBackend>,
        observer: Arc<io::IoObserver>,
    ) -> Self {
        Self {
            pools,
            lifecycle_registry,
            reader,
            file: io::segment_file::SegmentFileReader::new(
                io_mode,
                io_backend.clone(),
                None,
                false,
            ),
            io_backend,
            observer,
            write_modes: ParkingMutex::new(HashMap::new()),
            write_locks: ParkingMutex::new(HashMap::new()),
        }
    }

    /// Acquires the per-segment exclusive write guard.
    ///
    /// Multi-step read-modify-write flows (EC heal decode + splice)
    /// hold the guard across the whole sequence and rewrite through
    /// [`Self::write_segment_data_guarded`]; plain `write_segment_data`
    /// takes the same lock internally (never call it while holding the
    /// guard — the per-segment mutex is not reentrant).
    ///
    /// # Errors
    ///
    /// Never fails; the future resolves when the lock is acquired.
    pub async fn lock_segment(&self, segment_id: &SegmentId) -> SegmentWriteGuard {
        let lock = self
            .write_locks
            .lock()
            .entry(*segment_id)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone();
        let _guard = lock.lock_owned().await;
        SegmentWriteGuard { _guard, segment_id: *segment_id }
    }

    /// Writes a whole `.dat` under an already-held per-segment guard.
    ///
    /// The guard-holding form of the rewrite for multi-step
    /// read-modify-write flows (EC heal decode + splice): the caller
    /// takes [`Self::lock_segment`] across the whole sequence and calls
    /// this instead of the trait's `write_segment_data` (which would
    /// self-deadlock on the same non-reentrant per-segment mutex).
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] when the guard owns a
    /// different segment; otherwise the write errors of the plain
    /// whole-file write.
    pub async fn write_segment_data_guarded(
        &self,
        segment_id: &SegmentId,
        data: &[u8],
        guard: &SegmentWriteGuard,
    ) -> Result<()> {
        if guard.segment_id() != *segment_id {
            return Err(Error::InvalidArgument(format!(
                "segment write guard held for {} but the write targets {segment_id}",
                guard.segment_id()
            )));
        }
        self.write_unlocked(segment_id, data).await
    }

    /// The locked-free whole-file write: atomic temp → observed write →
    /// fsync → finalize on the segment's pool root, then purge-on-write.
    ///
    /// Callers MUST hold the segment's write lock (the trait
    /// `write_segment_data` or `write_segment_data_guarded` — never
    /// call this directly without owning the `.dat`).
    async fn write_unlocked(&self, segment_id: &SegmentId, data: &[u8]) -> Result<()> {
        let (root, pool_id) = self.resolve_pool(segment_id)?;
        std::fs::create_dir_all(&root)
            .map_err(|e| Error::Io(std::io::Error::other(format!("{e}"))))?;
        let filename = format!("{segment_id}.dat");

        // Synthesize a valid v1 header (76 bytes, no parity/index — the
        // read paths verify magic/version/size/checksum, so a zeroed
        // header would be rejected as corrupt). This is the header
        // heal/anti-entropy/re-rep repaired segments have always
        // carried.
        let header = v1_header_bytes(data);
        let write_mode = self.write_mode_for(pool_id, &root);
        let io_mode = self.file.read_mode;
        // Coerce the concrete observer to the pool-aware DiskIo's
        // observing surface (method-call receiver inference fixes T
        // before the unsizing coercion applies).
        let observer: Arc<dyn io::IoObserving> = self.observer.clone();
        let io: Arc<dyn DiskIo> =
            Arc::new(io::ObservedIo { pool_id, backend: Arc::clone(&self.io_backend), observer });
        let dir = root.clone();
        // Hygiene copies for the failure path (the originals move into
        // the blocking closure).
        let cleanup_dir = dir.clone();
        let cleanup_filename = filename.clone();
        // Owned copies cross the spawn_blocking boundary (borrows do
        // not); the payload copy is the same whole-file copy every
        // writer already performed.
        let owned = data.to_vec();

        // The blocking file section runs on the blocking pool — the
        // seal pipeline's discipline (the async method never performs
        // blocking I/O on a runtime worker).
        tokio::task::spawn_blocking(move || {
            write_dat_atomic(&dir, &filename, &header, &owned, io_mode, write_mode, io.as_ref())
        })
        .await
        .map_err(|e| Error::Internal(format!("segment write task failed for {segment_id}: {e}")))?
        .map_err(|e| {
            // Hygiene: remove a leftover `.tmp.{filename}` so failed
            // writes do not accumulate disk garbage (the unnamed
            // O_TMPFILE is reclaimed by the kernel on fd close).
            let _ =
                std::fs::remove_file(io::atomic_write::temp_path(&cleanup_dir, &cleanup_filename));
            Error::Io(std::io::Error::new(
                e.kind(),
                format!("segment write failed for {segment_id}: {e}"),
            ))
        })?;

        // Purge-on-write (ADR-0032 D2): the shared reader's per-segment
        // caches hold facts about the OLD file — header size, resolved
        // root, mmap mapping, read source. A rewritten `.dat` must be
        // re-verified by the next chunk read.
        self.reader.purge_cache(segment_id);
        Ok(())
    }

    /// Resolves the owning pool root + pool id for a registered segment.
    ///
    /// Registry-only (ADR-0031/0032 D3): a segment without a lifecycle
    /// entry has no pool — there is no write-before-register fallback.
    fn resolve_pool(&self, segment_id: &SegmentId) -> Result<(PathBuf, u32)> {
        let entry = self.lifecycle_registry.get(*segment_id).ok_or_else(|| {
            Error::Internal(format!(
                "segment {segment_id} is not registered in the lifecycle registry"
            ))
        })?;
        let pool_id = entry.metadata.pool_id;
        let root = self
            .pools
            .pool_by_id(pool_id)
            .map(|pool| pool.root().to_path_buf())
            .ok_or_else(|| {
                Error::Internal(format!("segment {segment_id} references unknown pool {pool_id}"))
            })?;
        Ok((root, pool_id))
    }

    /// Resolves a pool id to its live root (explicit-pool fast path).
    fn pool_root(&self, pool_id: u32, segment_id: &SegmentId) -> Result<PathBuf> {
        self.pools.pool_by_id(pool_id).map(|pool| pool.root().to_path_buf()).ok_or_else(|| {
            Error::Internal(format!("segment {segment_id} references unknown pool {pool_id}"))
        })
    }

    /// Probes (once per pool root) and returns the atomic write mode for
    /// a pool's filesystem.
    fn write_mode_for(&self, pool_id: u32, root: &Path) -> SegmentWriteMode {
        if let Some(mode) = self.write_modes.lock().get(&pool_id).copied() {
            return mode;
        }
        let mode = SegmentWriteMode::probe(root);
        self.write_modes.lock().insert(pool_id, mode);
        mode
    }

    /// Unlinks one `.dat` file and returns the reclaimed bytes (0 when
    /// no file existed — a missing file is not an error for deletes).
    fn unlink(&self, path: &Path) -> Result<u64> {
        let metadata = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(Error::Io(e)),
        };
        std::fs::remove_file(path).map_err(Error::Io)?;
        Ok(metadata)
    }
}

#[async_trait::async_trait]
impl SegmentDataStore for DiskSegmentStore {
    async fn read_segment_data(&self, segment_id: &SegmentId) -> Result<Option<SegmentFile>> {
        // Registry-only resolution. "No readable local `.dat`" — an
        // unregistered segment, OR a registered segment whose pool is
        // gone (f8 detach / dead pool) — reads as Ok(None): the read
        // coordinator treats a missing local copy as "fall back to the
        // segment's live replicas", which the re-replication dead-pool
        // scenario depends on. Erroring here would break that fallback.
        let entry = match self.lifecycle_registry.get(*segment_id) {
            Some(entry) => entry,
            None => return Ok(None),
        };
        let Some(pool) = self.pools.pool_by_id(entry.metadata.pool_id) else {
            return Ok(None);
        };
        let root = pool.root().to_path_buf();
        let path = root.join(format!("{segment_id}.dat"));
        // Verify (header-only parse — never repairs: scrub/AE must
        // OBSERVE corruption, not have it silently rewritten). One
        // parse serves the data-section geometry AND the SegmentFile
        // version/data_end contract.
        let header = match self.file.verify_header(segment_id, &path) {
            Ok(header) => header,
            Err(_) if !path.exists() => return Ok(None),
            Err(e) => {
                return Err(Error::Internal(format!("segment file {segment_id} unreadable: {e}")));
            }
        };
        let hdr_size = header.serialized_size();
        let data_len = u32::try_from(header.size).map_err(|_| {
            Error::Internal(format!(
                "segment file {segment_id} data section too large ({} bytes)",
                header.size
            ))
        })?;
        let data =
            self.file.read_range(segment_id, &path, hdr_size as u64, data_len).await.map_err(
                |e| Error::Internal(format!("segment read failed for {segment_id}: {e}")),
            )?;
        Ok(Some(SegmentFile {
            segment_id: *segment_id,
            version: header.version,
            header_len: hdr_size,
            data_end: header.data_end(),
            data,
        }))
    }

    async fn write_segment_data(&self, segment_id: &SegmentId, data: &[u8]) -> Result<()> {
        // Per-segment exclusivity (ADR-0032 D3): concurrent writers to
        // one `.dat` are unrepresentable.
        let _guard = self.lock_segment(segment_id).await;
        self.write_unlocked(segment_id, data).await
    }

    async fn delete_shards(&self, segment_id: &SegmentId) -> Result<u64> {
        // Registry-resolved delete. An unregistered segment (or one
        // whose entry is gone after a durable delete) returns 0 — the
        // orphan reaper's per-root sweep backstops any `.dat` residue
        // the registry can no longer place.
        let Ok((root, _)) = self.resolve_pool(segment_id) else {
            return Ok(0);
        };
        let path = root.join(format!("{segment_id}.dat"));
        self.unlink(&path)
    }

    async fn delete_shards_with_pool(&self, segment_id: &SegmentId, pool_id: u32) -> Result<u64> {
        // Explicit-pool fast path (GC compaction / reaper / remap): no
        // registry lookup.
        let root = self.pool_root(pool_id, segment_id)?;
        let path = root.join(format!("{segment_id}.dat"));
        self.unlink(&path)
    }

    fn list_segment_files(&self, root: &Path) -> Result<Vec<PathBuf>> {
        // Per-root sweep (ADR-0032 D1 shape): `.dat` files directly
        // under `root`. A missing root lists nothing.
        let entries = match std::fs::read_dir(root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::Io(e)),
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".dat") {
                out.push(entry.path());
            }
        }
        Ok(out)
    }
}

/// Synthesizes a valid v1 segment header (76 bytes) over `data`.
///
/// Layout: magic(4) + version(2) + size(8) + blob_count(4) +
/// index_offset(8) + checksum(32) — `index_offset` points at the end of
/// the data section (no index follows; readers slice
/// `[header .. index_offset]` and never touch an index for these
/// files).
fn v1_header_bytes(data: &[u8]) -> Vec<u8> {
    let mut header = vec![0u8; SEGMENT_HEADER_SIZE_V1];
    header[0..4].copy_from_slice(&SEGMENT_MAGIC);
    header[4..6].copy_from_slice(&1u16.to_le_bytes());
    header[22..30].copy_from_slice(&(data.len() as u64).to_le_bytes());
    header[30..34].copy_from_slice(&0u32.to_le_bytes()); // blob_count
    header[34..42].copy_from_slice(&((SEGMENT_HEADER_SIZE_V1 + data.len()) as u64).to_le_bytes());
    let checksum = *blake3::hash(data).as_bytes();
    header[42..74].copy_from_slice(&checksum);
    header
}

/// Writes a whole `.dat` atomically: header + data → fsync → finalize.
///
/// Mirrors the seal pipeline's temp-file discipline
/// ([`crate::io::atomic_write`]): never a bare `std::fs::write` of the
/// final path — the file becomes visible only after its data is
/// durable. Every write/fsync runs through the pool-aware observed
/// `DiskIo` (ADR-0029 §D3 per-pool signals).
///
/// `io_mode == Direct` (Linux) opens the temp file with `O_DIRECT`
/// (page-aligned buffer, 512-byte padded — the seal pipeline's direct
/// arm); otherwise `write_mode` picks `O_TMPFILE` (link) or
/// rename-based temp files.
fn write_dat_atomic(
    dir: &Path,
    filename: &str,
    header: &[u8],
    data: &[u8],
    io_mode: io::IoReadMode,
    write_mode: SegmentWriteMode,
    io: &dyn DiskIo,
) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        if io_mode == io::IoReadMode::Direct {
            use io::direct::OpenOptionsDirectExt;
            // O_DIRECT requires a 512-byte-aligned buffer AND a 512-byte
            // multiple I/O size. Build ONE aligned buffer, padded in
            // place (the seal pipeline's direct arm).
            const BLOCK_SIZE: usize = 512;
            let total = SEGMENT_HEADER_SIZE_V1 + data.len();
            let pad = (BLOCK_SIZE - (total % BLOCK_SIZE)) % BLOCK_SIZE;
            let mut aligned = io::DirectIoBuf::new(total + pad)?;
            let buf = aligned.as_bytes_mut();
            buf[0..header.len()].copy_from_slice(header);
            buf[header.len()..total].copy_from_slice(data);
            // `pad` bytes remain zero (DirectIoBuf is zero-initialised).

            let tmp = io::atomic_write::temp_path(dir, filename);
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .with_direct()
                .open(&tmp)?;
            io.write_handle(&file, aligned.as_bytes())?;
            io.fsync_handle(&file)?;
            return io::atomic_write::finalize_temp(SegmentWriteMode::Rename, file, dir, filename);
        }
    }

    // Buffered / O_TMPFILE path: create temp → write parts → fsync →
    // finalize (atomic visibility). Zero-copy: each part is written
    // directly from its source slice through the observed DiskIo.
    //
    // Overwrite semantics: a whole-file rewrite (heal) replaces an
    // existing `.dat`. `O_TMPFILE`'s linkat cannot overwrite (EEXIST),
    // so when the target exists the write degrades to the rename-based
    // temp path — `rename(2)` replaces atomically. Fresh files keep the
    // stronger never-visible-until-linked guarantee.
    let effective = if write_mode == SegmentWriteMode::Tmpfile && dir.join(filename).exists() {
        SegmentWriteMode::Rename
    } else {
        write_mode
    };
    let file = io::atomic_write::create_temp(effective, dir, filename)?;
    io.write_handle(&file, header)?;
    io.write_handle(&file, data)?;
    // Durability before visibility: fsync (fdatasync semantics) then
    // finalize — the flush coordinator's per-file barrier.
    io.fsync_handle(&file)?;
    io::atomic_write::finalize_temp(effective, file, dir, filename)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use oceanfs_core::{LifecycleConfig, SegmentId, StorageConfig, StoragePoolConfig};

    use super::*;
    use crate::{
        io::{IoBackend, IoObserver, IoReadMode, SegmentReader},
        pool::PoolRegistry,
        segment::{header::SegmentHeader, lifecycle::SegmentLifecycleCoordinator},
    };

    /// A pools-only store over one data pool (config-order id 0) plus
    /// the mandatory wal/metadata/hints siblings (ADR-0031), with a
    /// registered segment seeded through the coordinator (ADR-0025).
    struct TestEnv {
        _tmp: tempfile::TempDir,
        store: DiskSegmentStore,
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        data_root: std::path::PathBuf,
    }

    async fn make_env() -> TestEnv {
        let tmp = tempfile::tempdir().unwrap();
        let data_root = tmp.path().join("pool-data");
        let storage = StorageConfig {
            pools: vec![
                StoragePoolConfig {
                    name: "data-0".into(),
                    role: oceanfs_core::PoolRole::Data,
                    root: data_root.clone(),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                StoragePoolConfig {
                    name: "wal-0".into(),
                    role: oceanfs_core::PoolRole::Wal,
                    root: tmp.path().join("pool-wal"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                StoragePoolConfig {
                    name: "meta-0".into(),
                    role: oceanfs_core::PoolRole::Metadata,
                    root: tmp.path().join("pool-meta"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                StoragePoolConfig {
                    name: "hints-0".into(),
                    role: oceanfs_core::PoolRole::Hints,
                    root: tmp.path().join("pool-hints"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        };
        let pool_registry =
            Arc::new(PoolRegistry::from_config(&storage, &tmp.path().join("meta")).unwrap());
        let lifecycle_registry =
            Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        // The coordinator's durable writer: the event log is the only
        // durable writer (ADR-0025 Decision 3 — the CF fallback is
        // removed).
        let event_wal_dir = tmp.path().join("event-wal");
        let event_wal_config = oceanfs_core::EventWalConfig {
            event_wal_dir: event_wal_dir.clone(),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024 * 1024,
        };
        let event_wal = Arc::new(
            crate::segment::event_wal::EventWal::open(event_wal_dir.clone(), &event_wal_config)
                .await
                .unwrap(),
        );
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&lifecycle_registry))
                .with_event_wal(event_wal),
        );
        let observer = Arc::new(IoObserver::new());
        observer.register_pool(0, None);
        let reader: Arc<dyn SegmentReader> = Arc::new(crate::io::InMemorySegmentReader::new());
        let store = DiskSegmentStore::new(
            Arc::clone(&pool_registry),
            Arc::clone(&lifecycle_registry),
            reader,
            IoReadMode::Buffered,
            Arc::new(IoBackend::default()),
            observer,
        );
        TestEnv { _tmp: tmp, store, lifecycle, data_root }
    }

    /// Seeds a registered (reserved + sealed) segment.
    async fn seed(env: &TestEnv, data: &[u8]) -> SegmentId {
        let id = SegmentId::new();
        env.lifecycle.request_reserve(id, oceanfs_core::SizeTier::Standard, 4, 2).await.unwrap();
        // The event log requires a seal-time anchor root.
        let merkle_root = oceanfs_core::HashOutput::from_bytes(*blake3::hash(data).as_bytes());
        let meta = oceanfs_core::SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1_700_000_000_000),
        };
        env.lifecycle.request_seal(id, meta, None).await.unwrap();
        id
    }

    #[tokio::test]
    async fn io_layer_write_read_roundtrip_is_header_valid_v1() {
        let env = make_env().await;
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let id = seed(&env, &data).await;

        env.store.write_segment_data(&id, &data).await.unwrap();
        let file = env.store.read_segment_data(&id).await.unwrap().expect("present");
        assert_eq!(&file.data[..], &data[..]);
        assert_eq!(file.version, 1);
        assert_eq!(file.header_len, 76);
        assert_eq!(file.data_end as usize, 76 + data.len());

        // The file must parse as a valid v1 header under the strict
        // reader verification.
        let path = env.data_root.join(format!("{id}.dat"));
        let raw = std::fs::read(&path).unwrap();
        let header = SegmentHeader::from_bytes(&raw).expect("valid header");
        assert_eq!(header.version, 1);
        assert_eq!(header.data_end() as usize, 76 + data.len());
    }

    #[tokio::test]
    async fn missing_dat_reads_ok_none() {
        let env = make_env().await;
        let data = vec![1u8; 64];
        let id = seed(&env, &data).await;
        // Sealed but never written: no `.dat` yet.
        assert!(env.store.read_segment_data(&id).await.unwrap().is_none());
        // Unregistered: also Ok(None) — registry-only resolution.
        assert!(env.store.read_segment_data(&SegmentId::new()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn registry_pool_id_selects_the_pool_root() {
        let env = make_env().await;
        let data = vec![2u8; 128];
        let id = seed(&env, &data).await;
        env.store.write_segment_data(&id, &data).await.unwrap();
        // The entry names pool 0 → the file lands on pool-0's root (no
        // legacy fallback, ADR-0031).
        assert!(env.data_root.join(format!("{id}.dat")).exists());
    }

    #[tokio::test]
    async fn unregistered_write_is_rejected() {
        let env = make_env().await;
        let err = env
            .store
            .write_segment_data(&SegmentId::new(), b"no reserve")
            .await
            .expect_err("write-before-register must be rejected (ADR-0032 D3)");
        assert!(err.to_string().contains("not registered"), "{err}");
    }

    /// The multi-writer regression test: N tasks write distinct payloads
    /// to ONE `.dat` through ONE store; the per-segment lock serializes
    /// them, so the final file equals exactly one payload — never
    /// interleaved or partial bytes.
    #[tokio::test]
    async fn concurrent_writers_serialize_exactly_one_payload_survives() {
        let env = make_env().await;
        let id = seed(&env, b"").await;
        let store = Arc::new(env.store);
        let mut handles = Vec::new();
        for i in 0..8u8 {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                let payload = vec![i; 4096];
                store.write_segment_data(&id, &payload).await.unwrap();
                payload
            }));
        }
        let mut expected: Vec<Vec<u8>> = Vec::new();
        for h in handles {
            expected.push(h.await.unwrap());
        }
        let raw = std::fs::read(env.data_root.join(format!("{id}.dat"))).unwrap();
        let parsed = SegmentHeader::from_bytes(&raw).expect("valid header");
        let data_section = &raw[76..parsed.data_end() as usize];
        assert!(
            expected.iter().any(|payload| payload.as_slice() == data_section),
            "final file must equal exactly one writer's payload — got interleaved or partial bytes"
        );
    }

    #[tokio::test]
    async fn delete_and_list_work_per_pool_root() {
        let env = make_env().await;
        let data = vec![3u8; 512];
        let id = seed(&env, &data).await;
        env.store.write_segment_data(&id, &data).await.unwrap();
        let path = env.data_root.join(format!("{id}.dat"));
        assert!(path.exists());

        let listed = env.store.list_segment_files(&env.data_root).unwrap();
        assert_eq!(listed, vec![path.clone()]);

        // Resolver-based delete (registry entry names pool 0).
        let reclaimed = env.store.delete_shards(&id).await.unwrap();
        assert_eq!(reclaimed, 76 + data.len() as u64);
        assert!(!path.exists());

        // Re-delete of a missing file → 0 (not an error).
        assert_eq!(env.store.delete_shards(&id).await.unwrap(), 0);

        // Explicit-pool delete (GC fast path).
        env.store.write_segment_data(&id, &data).await.unwrap();
        assert_eq!(env.store.delete_shards_with_pool(&id, 0).await.unwrap(), 76 + 512);
        assert!(!path.exists());

        // Unknown pool ids are errors; missing roots list nothing.
        assert!(env.store.delete_shards_with_pool(&id, 42).await.is_err());
        assert!(env.store.list_segment_files(&env.data_root.join("absent")).unwrap().is_empty());
    }

    /// The guard-holding rewrite (ADR-0032 D3 RMW flows): a task holding
    /// `lock_segment` rewrites through `write_segment_data_guarded` while
    /// a concurrent task calls the plain trait write — the per-segment
    /// mutex serializes them, so the final file equals exactly one
    /// complete payload (never interleaved bytes).
    #[tokio::test]
    async fn guarded_rewrite_serializes_against_concurrent_plain_writer() {
        let env = make_env().await;
        let id = seed(&env, b"").await;
        let store = Arc::new(env.store);
        let writer_a = Arc::clone(&store);
        let writer_b = Arc::clone(&store);
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();

        let task_b = tokio::spawn(async move {
            // Plain trait write racing the guarded rewrite.
            let payload = vec![0xBB; 8192];
            writer_b.write_segment_data(&id, &payload).await.unwrap();
            payload
        });

        // A holds the guard, yields (letting B's write land or queue),
        // then rewrites through the guarded entry.
        let task_a = tokio::spawn(async move {
            let guard = writer_a.lock_segment(&id).await;
            let _ = start_tx.send(());
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let payload = vec![0xAA; 8192];
            writer_a.write_segment_data_guarded(&id, &payload, &guard).await.unwrap();
            payload
        });
        let _ = start_rx.await;
        let payload_a = task_a.await.unwrap();
        let payload_b = task_b.await.unwrap();

        let raw = std::fs::read(env.data_root.join(format!("{id}.dat"))).unwrap();
        let parsed = SegmentHeader::from_bytes(&raw).expect("valid header");
        let data_section = &raw[76..parsed.data_end() as usize];
        assert!(
            payload_a.as_slice() == data_section || payload_b.as_slice() == data_section,
            "final file must equal exactly one complete payload — guarded and plain writes serialized"
        );
    }

    /// The guard is segment-bound: rewriting a DIFFERENT segment through
    /// a held guard is rejected (a guard can never write a `.dat` it
    /// does not own).
    #[tokio::test]
    async fn guarded_write_rejects_wrong_segment() {
        let env = make_env().await;
        let owned = seed(&env, b"").await;
        let other = seed(&env, b"").await;
        let guard = env.store.lock_segment(&owned).await;
        let err = env
            .store
            .write_segment_data_guarded(&other, b"x", &guard)
            .await
            .expect_err("guard for another segment must be rejected");
        assert!(err.to_string().contains("guard held for"), "{err}");
        // The owned segment still rewrites fine through its guard.
        env.store
            .write_segment_data_guarded(&owned, b"y", &guard)
            .await
            .expect("guarded rewrite of the owned segment");
    }
}
