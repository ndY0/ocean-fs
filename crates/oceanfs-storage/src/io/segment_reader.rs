//! Segment reader — disk-backed and in-memory implementations.
//!
//! The [`SegmentReader`] trait is the abstraction for reading blob chunk
//! data from segments. Two implementations are provided:
//!
//! - [`DiskSegmentReader`] — reads from segment files on disk via the
//!   configured [`IoReadMode`] (mmap / O_DIRECT / buffered). Uses
//!   [`SegmentFileCache`] for zero-copy mmap reads and [`IoBackend`] for
//!   io_uring-accelerated I/O.
//!
//! - [`InMemorySegmentReader`] — stores segment data in a `HashMap`.
//!   Used for testing and as a fast path for recently-written segments
//!   that haven't been sealed to disk yet.
//! - [`PoolFallbackReader`] — composite reader that checks active
//!   (unsealed) segment buffers in one or more [`crate::segment::SegmentPool`]s
//!   before falling back to a disk-backed reader. Closes the
//!   read-after-write gap for recently-written data.
//!
//! ## Read source tracking
//!
//! [`SegmentReadSource`] accompanies each read result so the HTTP handler
//! can choose between `Body::from(Bytes)` (memory) and `SegmentFileBody`
//! (file-backed, for sendfile path).

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use bytes::Bytes;
use oceanfs_core::SegmentId;
use parking_lot::{Mutex, RwLock};

use super::{IoBackend, IoReadMode, SegmentFileCache};

// ---------------------------------------------------------------------------
// SegmentReader trait
// ---------------------------------------------------------------------------

/// Reads blob chunk data from segments.
///
/// The trait is async because disk-backed implementations need
/// asynchronous I/O. Use `#[async_trait::async_trait]` on impls.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_storage::io::SegmentReader;
/// use oceanfs_core::SegmentId;
///
/// # async fn example(reader: &dyn SegmentReader) {
/// let data = reader
///     .read_chunk(&SegmentId::new(), 0, 1024)
///     .await
///     .expect("read failed");
/// # }
/// ```
#[async_trait::async_trait]
pub trait SegmentReader: Send + Sync {
    /// Reads a chunk of data from a segment.
    ///
    /// Returns the chunk data as `Bytes`. The returned data may be
    /// backed by an mmap region (zero-copy) or a heap allocation.
    ///
    /// # Errors
    ///
    /// Returns an error string if the segment is not found or the
    /// read fails.
    async fn read_chunk(
        &self,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Bytes, String>;

    /// Returns the source metadata for the most recent `read_chunk` call.
    ///
    /// The default implementation returns [`SegmentReadSource::Memory`].
    /// Disk-backed readers override this to return file-backed information
    /// for sendfile integration.
    fn last_read_source(&self, _segment_id: &SegmentId) -> SegmentReadSource {
        SegmentReadSource::Memory
    }
}

// ---------------------------------------------------------------------------
// SegmentReadSource
// ---------------------------------------------------------------------------

/// Describes the data source for a segment chunk read.
///
/// Used by upper layers to choose the response body strategy:
/// - `Memory` → `Body::from(Bytes)` (zero-copy from Bytes)
/// - `MmapBacked` → `SegmentFileBody` (mmap-backed, sendfile path)
/// - `DirectIo` → `SegmentFileBody` or `Body::from(Bytes)` (both fine)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentReadSource {
    /// Data was served from an in-memory cache (HashMap, L1, inline
    /// metadata). The `Bytes` owns its data.
    Memory,
    /// Data was sliced from an mmap region backed by a segment file.
    /// The `Bytes` shares the mmap's `Arc` — zero additional allocation.
    MmapBacked {
        /// The segment that was mapped.
        segment_id: SegmentId,
        /// The file path on disk.
        file_path: PathBuf,
    },
    /// Data was read from disk via O_DIRECT or buffered I/O into a
    /// temporary buffer.
    DirectIo {
        /// The segment that was read.
        segment_id: SegmentId,
        /// The file path on disk.
        file_path: PathBuf,
    },
}

// ---------------------------------------------------------------------------
// DiskSegmentReader
// ---------------------------------------------------------------------------

/// Disk-backed segment reader implementing [`SegmentReader`].
///
/// Routes reads through the configured I/O backend based on
/// [`IoReadMode`], resolved from `NodeConfig::read_cache_segments`.
///
/// ## I/O Mode Selection
///
/// | `IoReadMode` | Read Path |
/// |---|---|
/// | `Mmap` | `SegmentFileCache::get_or_map()` → `&[u8]` slice → `Bytes` |
/// | `Direct` | `DirectIoBuf` → `IoBackend::read()` → `Bytes` |
/// | `Buffered` | `tokio::fs::File::read_at()` → `Bytes` |
///
/// ## Memory Bounds
///
/// Memory is bounded by the `SegmentFileCache` (max mmap entries ×
/// segment size) plus temporary `DirectIoBuf` allocations (per-read,
/// returned to pool). There is no unbounded HashMap.
pub struct DiskSegmentReader {
    /// The configured read mode, resolved at construction.
    read_mode: IoReadMode,
    /// The disk I/O backend (io_uring or tokio::fs).
    disk_io: Arc<IoBackend>,
    /// Optional LRU cache of memory-mapped segment files.
    mmap_cache: Option<Arc<SegmentFileCache>>,
    /// Data pool roots sealed segments are spread across (ADR-0029 f5).
    /// Empty = legacy mode: every segment resolves to `legacy_dir`.
    data_pools: Vec<Arc<crate::pool::StoragePool>>,
    /// The live `PoolRegistry` (f8 runtime attach), when wired: root
    /// resolution refreshes from it so a pool attached mid-run is
    /// readable immediately. `None` uses the boot-time `data_pools`.
    registry: Option<Arc<crate::pool::PoolRegistry>>,
    /// Legacy segments directory (pool_id 0 / no pools).
    legacy_dir: PathBuf,
    /// Resolves a segment's durable pool id (the lifecycle registry's
    /// `SegmentMetadata.pool_id`); `None`/0 → the legacy dir.
    pool_id_for: crate::pool::PoolIdResolver,
    /// Per-segment RESOLVED ROOT directory. The registry snapshot (a
    /// read lock) is taken ONCE per segment per process — on the first
    /// read, when the root is resolved; every subsequent read hits this
    /// cache, so there is no registry lock on the steady-state read
    /// path (f5 perf 7.2).
    pool_root_cache: parking_lot::Mutex<HashMap<SegmentId, PathBuf>>,
    /// Tracks the source of the most recent read, keyed by segment_id.
    last_source: Mutex<HashMap<SegmentId, SegmentReadSource>>,
    /// First-touch integrity state: maps segment_id → on-disk header
    /// size (the data section offset). Populated once per segment per
    /// process by [`verify_and_repair_segment`]; corrupt-but-repairable
    /// files are repaired on first touch.
    verified_headers: Mutex<HashMap<SegmentId, (usize, u64)>>,
    /// Injected EC decoder for corruption repair (the node wires the
    /// AccelDispatcher; None falls back to the plain Cauchy codec).
    ec_decoder: Option<std::sync::Arc<dyn oceanfs_ec::Decoder>>,
    /// Injected EC encoder for parity re-encode during repair.
    ec_encoder: Option<std::sync::Arc<dyn oceanfs_ec::Encoder>>,
    /// When `true`, call `madvise(MADV_DONTNEED)` after reading from mmap
    /// to eagerly evict segment data from the page cache. Set to `true`
    /// when `read_cache_segments = false` (write-optimised profile) so
    /// large segment reads don't pollute the page cache and evict hot
    /// metadata/WAL pages. No-op on non-Linux.
    evict_after_read: bool,
}

impl DiskSegmentReader {
    /// Creates a new disk-backed segment reader.
    ///
    /// `mmap_cache` should be `Some` when `read_mode == IoReadMode::Mmap`.
    /// Otherwise it is ignored. `segment_dir` is the legacy segments
    /// directory; call [`DiskSegmentReader::with_data_pools`] to enable
    /// multi-pool resolution (ADR-0029 f5).
    pub fn new(
        read_mode: IoReadMode,
        disk_io: Arc<IoBackend>,
        mmap_cache: Option<Arc<SegmentFileCache>>,
        segment_dir: PathBuf,
        ec_decoder: Option<std::sync::Arc<dyn oceanfs_ec::Decoder>>,
        ec_encoder: Option<std::sync::Arc<dyn oceanfs_ec::Encoder>>,
    ) -> Self {
        Self {
            read_mode,
            disk_io,
            mmap_cache,
            data_pools: Vec::new(),
            registry: None,
            legacy_dir: segment_dir,
            pool_id_for: Arc::new(|_| None),
            pool_root_cache: Mutex::new(HashMap::new()),
            ec_decoder,
            ec_encoder,
            last_source: Mutex::new(HashMap::new()),
            verified_headers: Mutex::new(HashMap::new()),
            evict_after_read: false,
        }
    }

    /// Enables pool-aware segment resolution (ADR-0029 f5).
    ///
    /// `data_pools` are the node's data pools (a snapshot); `legacy_dir`
    /// is the fallback for pool_id 0 and unknown ids; `pool_id_for`
    /// resolves a segment's durable pool id (the node backs it with the
    /// lifecycle registry's `SegmentMetadata.pool_id`). Reads then land
    /// on the owning pool root — plain joins over the pool snapshot, no
    /// locks in the read path.
    pub fn with_data_pools(
        mut self,
        data_pools: Vec<Arc<crate::pool::StoragePool>>,
        legacy_dir: PathBuf,
        pool_id_for: crate::pool::PoolIdResolver,
    ) -> Self {
        self.data_pools = data_pools;
        self.legacy_dir = legacy_dir;
        self.pool_id_for = pool_id_for;
        self
    }

    /// Wires the live `PoolRegistry` (f8 runtime attach).
    ///
    /// When set, root resolution refreshes the pool list from the
    /// registry so a pool attached via `POST /admin/pools` is readable
    /// immediately. The refresh happens once per segment (the first
    /// read caches the resolved root — f5 perf 7.2: no registry lock on
    /// the steady-state read path).
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// use oceanfs_storage::io::{IoBackend, DiskSegmentReader, IoReadMode};
    /// use oceanfs_storage::PoolRegistry;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let reader = DiskSegmentReader::new(
    ///     IoReadMode::Buffered,
    ///     Arc::new(IoBackend::default()),
    ///     None,
    ///     tmp.path().join("segments"),
    ///     None,
    ///     None,
    /// )
    /// .with_registry(Arc::new(registry));
    /// # let _ = reader;
    /// ```
    pub fn with_registry(mut self, registry: Arc<crate::pool::PoolRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Enables `madvise(MADV_DONTNEED)` after each mmap read to eagerly
    /// evict segment data from the page cache.
    ///
    /// Set to `true` when `read_cache_segments = false` (write-optimised
    /// profile). When `false` (read-optimised profile), segment data
    /// remains in the page cache for subsequent reads.
    pub fn with_evict_after_read(mut self, evict: bool) -> Self {
        self.evict_after_read = evict;
        self
    }

    /// Verifies (and repairs) the segment on first touch, returning the
    /// on-disk header size — the data section's byte offset.
    ///
    /// The whole-data checksum is verified once per segment per process;
    /// corrupt stripes are repaired from the stored EC parity
    /// ([`verify_and_repair_segment`]). Subsequent reads skip the
    /// verification.
    fn ensure_verified(&self, segment_id: SegmentId) -> std::result::Result<(usize, u64), String> {
        if let Some(cached) = self.verified_headers.lock().get(&segment_id).copied() {
            return Ok(cached);
        }
        let path = self.segment_path(&segment_id);
        let repaired = crate::segment::repair::verify_and_repair_segment(
            &path,
            self.ec_decoder.as_deref(),
            self.ec_encoder.as_deref(),
        )
        .map_err(|e| format!("integrity check failed for {segment_id}: {e}"))?;
        if repaired > 0 {
            // The mmap cache (if any) may hold the pre-repair mapping;
            // invalidate it so subsequent reads see the repaired bytes.
            if let Some(cache) = &self.mmap_cache {
                cache.invalidate(segment_id);
            }
        }
        // Parse the header (the repair already validated the file) to
        // learn the format version's data offset. Header-only read: the
        // previous std::fs::read loaded the whole segment just for the
        // 76-92 byte header — on first touches under load that was a
        // second full-file buffer per segment read (multi-GB anon bursts).
        use std::io::Read;
        let mut file =
            std::fs::File::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        let mut header_buf = [0u8; 128];
        let got =
            file.read(&mut header_buf).map_err(|e| format!("read {}: {e}", path.display()))?;
        if got < crate::segment::header::SegmentHeader::header_size(1) {
            return Err(format!("segment file {segment_id} too short for header"));
        }
        let header = crate::segment::header::SegmentHeader::from_bytes(&header_buf)
            .ok_or_else(|| format!("bad segment header for {segment_id}"))?;
        let hdr_size = header.serialized_size();
        let data_size = header.size;
        self.verified_headers.lock().insert(segment_id, (hdr_size, data_size));
        Ok((hdr_size, data_size))
    }

    /// Returns the filesystem path for a segment file — resolved through
    /// the segment's durable pool id (ADR-0029 f5); legacy (no pools /
    /// pool_id 0 / unknown id) resolves to the legacy segments dir. The
    /// pool-id lookup is cached per segment, so the registry is touched
    /// once per segment per process (f5 perf 7.2: no registry lock on
    /// the read path).
    fn segment_path(&self, segment_id: &SegmentId) -> PathBuf {
        let root = if self.data_pools.is_empty() {
            self.legacy_dir.clone()
        } else {
            // Cache lookup: the guard from the first lock() is dropped at
            // the end of THIS statement — never hold it across the
            // resolver call or the second lock() (parking_lot Mutex is
            // not reentrant).
            let cached = self.pool_root_cache.lock().get(segment_id).cloned();
            match cached {
                Some(root) => root,
                None => {
                    let pool_id = (self.pool_id_for)(segment_id).unwrap_or(0);
                    // f8: refresh the pool list from the live registry
                    // when wired (a runtime-attached pool resolves here);
                    // the resolved root is cached, so the registry read
                    // lock is taken once per segment per process (f5
                    // perf 7.2 — no registry lock on the read path).
                    let pools: Vec<Arc<crate::pool::StoragePool>> = match &self.registry {
                        Some(registry) => registry.data_pools(),
                        None => self.data_pools.clone(),
                    };
                    // `resolve_pool_root` is pools-only since legacy f2
                    // (ADR-0031 D2): an id no registered pool carries
                    // falls back to this reader's own legacy dir — the
                    // reader's internal legacy branch is theme-1 store
                    // unification territory, not removed here.
                    let root = crate::pool::resolve_pool_root(&pools, pool_id)
                        .unwrap_or_else(|| self.legacy_dir.clone());
                    self.pool_root_cache.lock().insert(*segment_id, root.clone());
                    root
                }
            }
        };
        root.join(format!("{segment_id}.dat"))
    }
}

#[async_trait::async_trait]
impl SegmentReader for DiskSegmentReader {
    async fn read_chunk(
        &self,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Bytes, String> {
        let path = self.segment_path(segment_id);
        // First touch: verify integrity and learn the format version's
        // data offset (v1 = 76 bytes, v2 = 92 bytes). Blob offsets are
        // relative to the data region, AFTER the segment header.
        let (hdr_size, data_size) = self.ensure_verified(*segment_id).map_err(|e| e.to_string())?;
        // u32::MAX is the EC recovery's "whole segment" sentinel — the
        // reader resolves it to the file's actual data size. Treating
        // it literally would allocate/read 4 GiB from a small file.
        let length = if length == u32::MAX { data_size as u32 } else { length };
        let file_offset = offset + hdr_size as u64;

        let (data, source) = match self.read_mode {
            IoReadMode::Mmap => {
                if let Some(ref cache) = self.mmap_cache {
                    match cache.get_or_map(*segment_id, &path) {
                        Ok(mmap) => {
                            let start = file_offset as usize;
                            let end = start.saturating_add(length as usize).min(mmap.len());
                            #[cfg(target_os = "linux")]
                            {
                                // Tell the kernel this is a sequential forward scan
                                // so it can do aggressive read-ahead.
                                let _ = madvise_sequential(mmap.as_ptr(), mmap.len());
                            }
                            #[cfg(not(target_os = "linux"))]
                            {
                                let _ = mmap.len(); // suppress unused warning
                            }
                            let slice = &mmap[start..end];
                            let data = Bytes::copy_from_slice(slice);
                            #[cfg(target_os = "linux")]
                            {
                                // Eagerly evict pages from the page cache so segment
                                // reads don't push hot metadata/WAL data out of cache.
                                // Only when the write-optimised profile is in use
                                // (read_cache_segments=false). When caching is enabled,
                                // we want pages to stay resident.
                                if self.evict_after_read {
                                    let _ = madvise_dontneed(mmap.as_ptr(), mmap.len());
                                }
                            }
                            let source = SegmentReadSource::MmapBacked {
                                segment_id: *segment_id,
                                file_path: path.clone(),
                            };
                            (data, source)
                        }
                        Err(e) => {
                            return Err(format!("mmap read failed for {segment_id}: {e}"));
                        }
                    }
                } else {
                    // Mmap mode but no cache configured — fall back to buffered.
                    read_buffered(segment_id, &path, file_offset, length).await?
                }
            }
            IoReadMode::Direct => {
                let len = length as usize;
                let mut buf = crate::io::DirectIoBuf::new(len)
                    .map_err(|e| format!("DirectIoBuf allocation failed for {segment_id}: {e}"))?;
                // `IoBackend::read` performs a single read syscall per call.
                // `tokio::fs::File` caps a single read at 2 MiB, so a
                // larger request returns short — loop until the buffer
                // is full (read-path-integrity-under-load: the ignored
                // short read previously zero-padded every >2 MiB chunk,
                // producing BadDigest on every multi-tier read).
                let mut filled: usize = 0;
                while filled < len {
                    let n = self
                        .disk_io
                        .read(&path, &mut buf.as_bytes_mut()[filled..], file_offset + filled as u64)
                        .await
                        .map_err(|e| format!("Direct read failed for {segment_id}: {e}"))?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                if filled < len {
                    return Err(format!(
                        "Direct read short for {segment_id}: got {filled} of {len} bytes"
                    ));
                }
                let data = Bytes::copy_from_slice(&buf.as_bytes()[..len]);
                let source = SegmentReadSource::DirectIo {
                    segment_id: *segment_id,
                    file_path: path.clone(),
                };
                (data, source)
            }
            IoReadMode::Buffered => read_buffered(segment_id, &path, file_offset, length).await?,
        };

        self.last_source.lock().insert(*segment_id, source);
        Ok(data)
    }

    fn last_read_source(&self, segment_id: &SegmentId) -> SegmentReadSource {
        self.last_source.lock().get(segment_id).cloned().unwrap_or(SegmentReadSource::Memory)
    }
}

/// Buffered read fallback using `tokio::fs`.
async fn read_buffered(
    segment_id: &SegmentId,
    path: &std::path::Path,
    offset: u64,
    length: u32,
) -> std::result::Result<(Bytes, SegmentReadSource), String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("failed to open segment file {segment_id}: {e}"))?;
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| format!("seek failed for {segment_id}: {e}"))?;
    let mut buf = vec![0u8; length as usize];
    file.read_exact(&mut buf)
        .await
        .map_err(|e| format!("buffered read failed for {segment_id}: {e}"))?;
    let source =
        SegmentReadSource::DirectIo { segment_id: *segment_id, file_path: path.to_path_buf() };
    Ok((Bytes::from(buf), source))
}

// ---------------------------------------------------------------------------
// InMemorySegmentReader (moved from coordinator.rs, for tests)
// ---------------------------------------------------------------------------

/// In-memory segment reader for testing and fast-path reads.
///
/// Stores full segment data in a `HashMap<SegmentId, Bytes>`.
/// Does not read from disk — all data must be pre-loaded via [`put`].
///
/// [`put`]: InMemorySegmentReader::put
pub struct InMemorySegmentReader {
    segments: RwLock<HashMap<SegmentId, Bytes>>,
}

impl InMemorySegmentReader {
    /// Creates an empty in-memory segment reader.
    pub fn new() -> Self {
        Self { segments: RwLock::new(HashMap::new()) }
    }

    /// Stores segment data in the in-memory store.
    pub fn put(&self, segment_id: SegmentId, data: Bytes) {
        self.segments.write().insert(segment_id, data);
    }

    /// Returns the number of segments stored.
    pub fn len(&self) -> usize {
        self.segments.read().len()
    }

    /// Returns `true` if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for InMemorySegmentReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SegmentReader for InMemorySegmentReader {
    async fn read_chunk(
        &self,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Bytes, String> {
        let segments = self.segments.read();
        let full = segments
            .get(segment_id)
            .cloned()
            .ok_or_else(|| format!("segment {segment_id} not found in memory"))?;

        let start = offset as usize;
        let end = start.saturating_add(length as usize).min(full.len());
        Ok(full.slice(start..end))
    }
}

// ---------------------------------------------------------------------------
// PoolFallbackReader — checks active segments before falling back
// ---------------------------------------------------------------------------

/// A composite [`SegmentReader`] that first checks active (unsealed) segments
/// in one or more [`crate::segment::SegmentPool`]s, then falls back to a
/// disk-backed reader.
///
/// This closes the read-after-write gap: data acknowledged by a PUT may still
/// reside in an active segment buffer and not yet be sealed to disk. Without
/// this composite, GET requests would return 500 for recently-written objects.
///
/// The pool check is synchronous and lock-free in practice — it acquires the
/// same `parking_lot::Mutex` used by `append`, copies the byte range, and
/// releases. No async I/O, no scheduling yields.
pub struct PoolFallbackReader {
    /// Segment pools to check before falling back to disk.
    pools: Vec<Arc<crate::segment::SegmentPool>>,
    /// The fallback reader (typically a [`DiskSegmentReader`]).
    fallback: Arc<dyn SegmentReader>,
}

impl PoolFallbackReader {
    /// Creates a new composite reader.
    ///
    /// `pools` are searched in order. The first pool containing a matching
    /// `segment_id` wins. `fallback` is consulted only when no pool match.
    pub fn new(
        pools: Vec<Arc<crate::segment::pool::SegmentPool>>,
        fallback: Arc<dyn SegmentReader>,
    ) -> Self {
        Self { pools, fallback }
    }
}

#[async_trait::async_trait]
impl SegmentReader for PoolFallbackReader {
    async fn read_chunk(
        &self,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Bytes, String> {
        // Check active pools first — synchronous, microsecond-scale.
        for pool in &self.pools {
            if let Some(data) = pool.try_read(*segment_id, offset, length) {
                return Ok(data);
            }
        }
        // Fall back to disk-backed reader.
        self.fallback.read_chunk(segment_id, offset, length).await
    }

    fn last_read_source(&self, segment_id: &SegmentId) -> SegmentReadSource {
        self.fallback.last_read_source(segment_id)
    }
}

// ---------------------------------------------------------------------------
// madvise helpers (Linux only)
// ---------------------------------------------------------------------------

/// Calls `madvise(addr, len, MADV_SEQUENTIAL)` to hint that the mapped
/// region will be accessed sequentially — the kernel can do aggressive
/// read-ahead. No-op on non-Linux.
#[cfg(target_os = "linux")]
fn madvise_sequential(addr: *const u8, len: usize) -> std::io::Result<()> {
    // SAFETY: `addr` points to a valid memory-mapped region of `len` bytes.
    // `MADV_SEQUENTIAL` is a pure hint — it cannot cause UB even if the
    // addresses are invalid (the kernel may ignore the hint).
    #[allow(unsafe_code)]
    let ret = unsafe { libc::madvise(addr as *mut libc::c_void, len, libc::MADV_SEQUENTIAL) };
    if ret != 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Calls `madvise(addr, len, MADV_DONTNEED)` to hint that the mapped
/// region will not be accessed again soon — the kernel can eagerly evict
/// these pages from the page cache. No-op on non-Linux.
#[cfg(target_os = "linux")]
fn madvise_dontneed(addr: *const u8, len: usize) -> std::io::Result<()> {
    // SAFETY: `addr` points to a valid memory-mapped region of `len` bytes.
    // `MADV_DONTNEED` is advisory — it tells the kernel to drop these pages
    // from the page cache. If the address is invalid, the kernel returns
    // an error but does not cause UB.
    #[allow(unsafe_code)]
    let ret = unsafe { libc::madvise(addr as *mut libc::c_void, len, libc::MADV_DONTNEED) };
    if ret != 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{LifecycleConfig, PoolConfig, SegmentSizeConfig, SizeTier};

    use super::*;
    use crate::{
        buffer_pool::BufferPool,
        segment::{SegmentLifecycleRegistry, SegmentPool},
    };

    fn temp_segment_file(dir: &tempfile::TempDir, id: SegmentId) -> PathBuf {
        temp_segment_file_with_data(dir, id, &vec![0xABu8; 4096])
    }

    /// Writes a valid v1 segment file (76-byte header with a real
    /// checksum, version 1, no parity) followed by `data`.
    fn temp_segment_file_with_data(dir: &tempfile::TempDir, id: SegmentId, data: &[u8]) -> PathBuf {
        const V1_HEADER_SIZE: usize = crate::segment::header::SEGMENT_HEADER_SIZE_V1;
        let path = dir.path().join(format!("{id}.dat"));
        let checksum = *blake3::hash(data).as_bytes();
        let header = crate::segment::header::SegmentHeader {
            magic: crate::segment::header::SEGMENT_MAGIC,
            version: crate::segment::header::SEGMENT_VERSION_V1,
            segment_id: id,
            size: data.len() as u64,
            blob_count: 0,
            index_offset: (V1_HEADER_SIZE + data.len()) as u64,
            checksum,
            parity_offset: 0,
            parity_size: 0,
        };
        let mut file_data = vec![0u8; V1_HEADER_SIZE];
        file_data[0..4].copy_from_slice(&header.magic);
        file_data[4..6].copy_from_slice(&header.version.to_le_bytes());
        file_data[6..22].copy_from_slice(id.as_uuid().as_bytes());
        file_data[22..30].copy_from_slice(&header.size.to_le_bytes());
        file_data[30..34].copy_from_slice(&header.blob_count.to_le_bytes());
        file_data[34..42].copy_from_slice(&header.index_offset.to_le_bytes());
        file_data[42..74].copy_from_slice(&header.checksum);
        file_data.extend_from_slice(data);
        std::fs::write(&path, &file_data).unwrap();
        path
    }

    // --- InMemorySegmentReader tests ---

    #[tokio::test]
    async fn in_memory_read_chunk_returns_correct_slice() {
        let reader = InMemorySegmentReader::new();
        let id = SegmentId::new();
        reader.put(id, Bytes::from_static(&[0, 1, 2, 3, 4, 5, 6, 7]));

        let chunk = reader.read_chunk(&id, 2, 4).await.unwrap();
        assert_eq!(&chunk[..], &[2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn in_memory_read_chunk_missing_segment_returns_err() {
        let reader = InMemorySegmentReader::new();
        let result = reader.read_chunk(&SegmentId::new(), 0, 100).await;
        assert!(result.is_err());
    }

    // --- DiskSegmentReader tests ---

    #[tokio::test]
    async fn disk_reader_buffered_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let _ = temp_segment_file(&dir, id);

        let reader = DiskSegmentReader::new(
            IoReadMode::Buffered,
            Arc::new(IoBackend::TokioFs),
            None,
            dir.path().to_path_buf(),
            None,
            None,
        );

        let data = reader.read_chunk(&id, 0, 100).await.unwrap();
        assert_eq!(data.len(), 100);
        assert_eq!(data[0], 0xAB);
    }

    #[tokio::test]
    async fn disk_reader_mmap_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let _ = temp_segment_file(&dir, id);

        let cache = Arc::new(SegmentFileCache::new(4));
        let reader = DiskSegmentReader::new(
            IoReadMode::Mmap,
            Arc::new(IoBackend::TokioFs),
            Some(cache),
            dir.path().to_path_buf(),
            None,
            None,
        );

        let data = reader.read_chunk(&id, 0, 512).await.unwrap();
        assert_eq!(data.len(), 512);
        assert_eq!(data[0], 0xAB);

        // Source should be mmap-backed.
        let source = reader.last_read_source(&id);
        assert!(matches!(source, SegmentReadSource::MmapBacked { .. }));
    }

    #[tokio::test]
    async fn disk_reader_mmap_cache_hit_second_read() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let _ = temp_segment_file(&dir, id);

        let cache = Arc::new(SegmentFileCache::new(4));
        let reader = DiskSegmentReader::new(
            IoReadMode::Mmap,
            Arc::new(IoBackend::TokioFs),
            Some(cache.clone()),
            dir.path().to_path_buf(),
            None,
            None,
        );

        let _data1 = reader.read_chunk(&id, 0, 100).await.unwrap();
        assert_eq!(cache.len(), 1);

        let _data2 = reader.read_chunk(&id, 0, 100).await.unwrap();
        assert_eq!(cache.len(), 1); // Still one entry — cache hit.
    }

    #[tokio::test]
    async fn disk_reader_missing_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let reader = DiskSegmentReader::new(
            IoReadMode::Buffered,
            Arc::new(IoBackend::TokioFs),
            None,
            dir.path().to_path_buf(),
            None,
            None,
        );

        let result = reader.read_chunk(&SegmentId::new(), 0, 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn disk_reader_direct_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let _ = temp_segment_file(&dir, id);

        let reader = DiskSegmentReader::new(
            IoReadMode::Direct,
            Arc::new(IoBackend::TokioFs),
            None,
            dir.path().to_path_buf(),
            None,
            None,
        );

        let data = reader.read_chunk(&id, 0, 256).await.unwrap();
        assert_eq!(data.len(), 256);
        assert_eq!(data[0], 0xAB);

        let source = reader.last_read_source(&id);
        assert!(matches!(source, SegmentReadSource::DirectIo { .. }));
    }

    #[tokio::test]
    async fn disk_reader_large_read_across_mode() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        // Write a valid v1 segment file whose data section is 64 KB of
        // 0xCD; reads use offsets relative to the data section.
        let segment_data = vec![0xCDu8; 65536]; // 64 KB
        let _path = temp_segment_file_with_data(&dir, id, &segment_data);

        for &mode in &[IoReadMode::Buffered, IoReadMode::Direct] {
            let reader = DiskSegmentReader::new(
                mode,
                Arc::new(IoBackend::TokioFs),
                None,
                dir.path().to_path_buf(),
                None,
                None,
            );

            let data = reader.read_chunk(&id, 1024, 8192).await.unwrap();
            assert_eq!(data.len(), 8192);
            assert!(data.iter().all(|&b| b == 0xCD));
        }
    }

    #[tokio::test]
    async fn disk_reader_direct_reads_larger_than_2mib() {
        // tokio::fs::File caps a single read syscall at 2 MiB — a chunk
        // read larger than that must still return complete, correct data.
        // Regression test for read-path-integrity-under-load (the Direct
        // arm previously ignored the short-read count and zero-padded
        // every read beyond 2 MiB, producing BadDigest on multi-tier GETs).
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let payload: Vec<u8> = (0..3_200_000u32).map(|i| (i % 251) as u8).collect();
        let _ = temp_segment_file_with_data(&dir, id, &payload);

        let reader = DiskSegmentReader::new(
            IoReadMode::Direct,
            Arc::new(IoBackend::TokioFs),
            None,
            dir.path().to_path_buf(),
            None,
            None,
        );

        let data = reader.read_chunk(&id, 0, payload.len() as u32).await.unwrap();
        assert_eq!(data.len(), payload.len());
        assert_eq!(&data[..], &payload[..], "Direct-mode read must not zero-pad past 2 MiB");
    }

    // --- PoolFallbackReader tests ---

    fn test_pool() -> Arc<SegmentPool> {
        let pool_cfg = PoolConfig::default();
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 32));
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                registry,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn pool_fallback_reader_hits_active_segment() {
        let pool = test_pool();
        let fallback = Arc::new(InMemorySegmentReader::new());

        // Write data into the active pool. The machine entry must
        // exist for the read resolution (the write path reserves before
        // any readable state) — mirror that contract here.
        let data = b"active segment test data";
        let (seg_id, offset, length) = pool.append(data).unwrap();
        pool.lifecycle_registry()
            .reserve(
                seg_id,
                oceanfs_core::SegmentMetadata {
                    pool_id: 0,
                    segment_id: seg_id,
                    ec_k: 0,
                    ec_m: 0,
                    size_tier: SizeTier::Standard,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                },
            )
            .unwrap();

        let reader = PoolFallbackReader::new(vec![pool], fallback);

        // Should hit the pool, not the fallback.
        let chunk = reader.read_chunk(&seg_id, offset, length).await.unwrap();
        assert_eq!(&chunk[..], data);
    }

    #[tokio::test]
    async fn pool_fallback_reader_falls_back_when_pool_misses() {
        let pool = test_pool();
        let fallback = Arc::new(InMemorySegmentReader::new());

        // Write data into the fallback, not the pool.
        let fallback_id = SegmentId::new();
        fallback.put(fallback_id, Bytes::from_static(&[1, 2, 3, 4, 5]));

        let reader = PoolFallbackReader::new(vec![pool], fallback);

        // Pool doesn't have this segment — should hit fallback.
        let chunk = reader.read_chunk(&fallback_id, 1, 3).await.unwrap();
        assert_eq!(&chunk[..], &[2, 3, 4]);
    }

    #[tokio::test]
    async fn pool_fallback_reader_returns_error_when_both_miss() {
        let pool = test_pool();
        let fallback = Arc::new(InMemorySegmentReader::new());

        let reader = PoolFallbackReader::new(vec![pool], fallback);

        // Neither pool nor fallback have this segment.
        let result = reader.read_chunk(&SegmentId::new(), 0, 10).await;
        assert!(result.is_err());
    }
}
