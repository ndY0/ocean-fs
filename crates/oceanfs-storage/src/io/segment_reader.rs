//! Segment reader — disk-backed and in-memory implementations.
//!
//! The [`SegmentReader`] trait is the abstraction for reading blob chunk
//! data from segments. Two implementations are provided:
//!
//! - [`DiskSegmentReader`] — reads from segment files on disk via the
//!   configured [`IoReadMode`] (mmap / O_DIRECT / buffered). Uses
//!   [`SegmentFileCache`] for zero-copy mmap reads and [`DiskIo`] for
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

use super::{DiskIo, IoReadMode, SegmentFileCache};
use crate::segment::header::SEGMENT_HEADER_SIZE;

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
/// | `Direct` | `DirectIoBuf` → `DiskIo::read()` → `Bytes` |
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
    disk_io: Arc<DiskIo>,
    /// Optional LRU cache of memory-mapped segment files.
    mmap_cache: Option<Arc<SegmentFileCache>>,
    /// Base directory for segment files.
    segment_dir: PathBuf,
    /// Tracks the source of the most recent read, keyed by segment_id.
    last_source: Mutex<HashMap<SegmentId, SegmentReadSource>>,
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
    /// Otherwise it is ignored.
    pub fn new(
        read_mode: IoReadMode,
        disk_io: Arc<DiskIo>,
        mmap_cache: Option<Arc<SegmentFileCache>>,
        segment_dir: PathBuf,
    ) -> Self {
        Self {
            read_mode,
            disk_io,
            mmap_cache,
            segment_dir,
            last_source: Mutex::new(HashMap::new()),
            evict_after_read: false,
        }
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

    /// Returns the filesystem path for a segment file.
    fn segment_path(&self, segment_id: &SegmentId) -> PathBuf {
        self.segment_dir.join(format!("{segment_id}.dat"))
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
        // Blob offsets are relative to the data region, AFTER the
        // 76-byte segment header. Convert to file-level offset.
        let file_offset = offset + SEGMENT_HEADER_SIZE as u64;

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
                // `DiskIo::read` performs a single read syscall per call.
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
    use oceanfs_core::{PoolConfig, SegmentSizeConfig, SizeTier};

    use super::*;
    use crate::{buffer_pool::BufferPool, segment::SegmentPool};

    fn temp_segment_file(dir: &tempfile::TempDir, id: SegmentId) -> PathBuf {
        let path = dir.path().join(format!("{id}.dat"));
        // Write a minimal segment file with a 76-byte header followed by
        // the test data. The header is zeroed except for magic bytes.
        let header = vec![0u8; SEGMENT_HEADER_SIZE];
        let data = vec![0xABu8; 4096];
        let mut file_data = header;
        file_data.extend_from_slice(&data);
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
            Arc::new(DiskIo::TokioFs),
            None,
            dir.path().to_path_buf(),
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
            Arc::new(DiskIo::TokioFs),
            Some(cache),
            dir.path().to_path_buf(),
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
            Arc::new(DiskIo::TokioFs),
            Some(cache.clone()),
            dir.path().to_path_buf(),
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
            Arc::new(DiskIo::TokioFs),
            None,
            dir.path().to_path_buf(),
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
            Arc::new(DiskIo::TokioFs),
            None,
            dir.path().to_path_buf(),
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
        let path = dir.path().join(format!("{id}.dat"));
        let segment_data = vec![0xCDu8; 65536]; // 64 KB
        std::fs::write(&path, &segment_data).unwrap();

        for &mode in &[IoReadMode::Buffered, IoReadMode::Direct] {
            let reader = DiskSegmentReader::new(
                mode,
                Arc::new(DiskIo::TokioFs),
                None,
                dir.path().to_path_buf(),
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
        let path = dir.path().join(format!("{id}.dat"));
        let payload: Vec<u8> = (0..3_200_000u32).map(|i| (i % 251) as u8).collect();
        let mut file_data = vec![0u8; SEGMENT_HEADER_SIZE];
        file_data.extend_from_slice(&payload);
        std::fs::write(&path, &file_data).unwrap();

        let reader = DiskSegmentReader::new(
            IoReadMode::Direct,
            Arc::new(DiskIo::TokioFs),
            None,
            dir.path().to_path_buf(),
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
        Arc::new(SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap())
    }

    #[tokio::test]
    async fn pool_fallback_reader_hits_active_segment() {
        let pool = test_pool();
        let fallback = Arc::new(InMemorySegmentReader::new());

        // Write data into the active pool.
        let data = b"active segment test data";
        let (seg_id, offset, length) = pool.append(data).unwrap();

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
