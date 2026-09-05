//! Path-agnostic segment file read core (ADR-0032 D2).
//!
//! The single implementation of the optimized segment-file read
//! mechanics: header verification (v1 76-byte / v2 92-byte) and
//! ranged reads dispatched over mmap / O_DIRECT-style / buffered per
//! [`IoReadMode`]. Both the server chunk reader
//! ([`super::segment_reader::DiskSegmentReader`]) and the unified
//! whole-file store ([`crate::segment::data_store::DiskSegmentStore`])
//! perform their `.dat` reads through this core — one implementation of
//! the I/O mechanics, no divergent reader logic (the pre-unification
//! review: "two divergent data readers" / "3 abstractions to access
//! disk, each independently implements optimisations or not").
//!
//! The core is deliberately *path-agnostic*: resolution (pool root vs
//! legacy dir) is a caller concern — the chunk reader caches its
//! per-segment resolution, the store resolves registry-only. The core
//! takes a concrete file path and a segment id (for cache keying and
//! error messages).

use std::{path::Path, sync::Arc};

use bytes::Bytes;
use oceanfs_core::SegmentId;

use super::{IoBackend, IoReadMode, SegmentFileCache};

/// The shared file-level read core.
///
/// Holds exactly the I/O mechanics — read mode, backend, optional mmap
/// LRU, page-cache policy — and nothing about *which* segment maps to
/// *which* path. `mmap_cache` is `Some` only for callers that want
/// whole-file mappings retained (the server chunk reader); whole-file
/// scan readers (the store) leave it `None` so their reads never
/// populate the bounded LRU.
pub(crate) struct SegmentFileReader {
    /// The configured read mode, resolved at construction.
    pub(crate) read_mode: IoReadMode,
    /// The disk I/O backend (io_uring or tokio::fs).
    pub(crate) disk_io: Arc<IoBackend>,
    /// Optional LRU cache of memory-mapped segment files.
    pub(crate) mmap_cache: Option<Arc<SegmentFileCache>>,
    /// When `true`, call `madvise(MADV_DONTNEED)` after reading from
    /// mmap to eagerly evict segment data from the page cache. No-op on
    /// non-Linux.
    pub(crate) evict_after_read: bool,
}

impl SegmentFileReader {
    /// Creates the read core.
    ///
    /// `mmap_cache` should be `Some` when `read_mode == IoReadMode::Mmap`
    /// and the caller wants whole-file mappings cached (server chunk
    /// path). Without a cache, `Mmap` mode falls back to buffered reads.
    pub(crate) fn new(
        read_mode: IoReadMode,
        disk_io: Arc<IoBackend>,
        mmap_cache: Option<Arc<SegmentFileCache>>,
        evict_after_read: bool,
    ) -> Self {
        Self { read_mode, disk_io, mmap_cache, evict_after_read }
    }

    /// Returns `true` when reads go through the mmap LRU (the chunk
    /// reader uses this to report [`SegmentReadSource`]-accurate
    /// metadata).
    ///
    /// [`SegmentReadSource`]: super::segment_reader::SegmentReadSource
    pub(crate) fn serves_from_mmap_cache(&self) -> bool {
        matches!(self.read_mode, IoReadMode::Mmap) && self.mmap_cache.is_some()
    }

    /// Parses a `.dat`'s header (header-only read).
    ///
    /// Deliberately does NOT repair: repair-on-first-touch is the
    /// server chunk path's policy (it owns the EC codecs); integrity
    /// *detection* consumers (scrub/AE) must observe corruption, not
    /// have it silently rewritten. Callers derive the data section as
    /// `[serialized_size .. serialized_size + size]` (for v2-parity
    /// files the parity offset lies at `header + size`, so that range
    /// is the data section in every layout).
    ///
    /// # Errors
    ///
    /// Returns an error string when the file cannot be opened, is too
    /// short for a header, or carries a bad header.
    pub(crate) fn verify_header(
        &self,
        segment_id: &SegmentId,
        path: &Path,
    ) -> std::result::Result<crate::segment::header::SegmentHeader, String> {
        use std::io::Read;
        let mut file =
            std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        let mut header_buf = [0u8; 128];
        let got =
            file.read(&mut header_buf).map_err(|e| format!("read {}: {e}", path.display()))?;
        if got < crate::segment::header::SegmentHeader::header_size(1) {
            return Err(format!("segment file {segment_id} too short for header"));
        }
        crate::segment::header::SegmentHeader::from_bytes(&header_buf)
            .ok_or_else(|| format!("bad segment header for {segment_id}"))
    }

    /// Reads `length` bytes at `file_offset` (FILE coordinates — the
    /// caller adds the header size to data-relative offsets) from the
    /// `.dat` at `path`, dispatching per [`IoReadMode`].
    ///
    /// The caller supplies `segment_id` for mmap-cache keying and error
    /// messages.
    ///
    /// # Errors
    ///
    /// Returns an error string if the read fails (open, short read,
    /// mmap failure, ...).
    pub(crate) async fn read_range(
        &self,
        segment_id: &SegmentId,
        path: &Path,
        file_offset: u64,
        length: u32,
    ) -> std::result::Result<Bytes, String> {
        match self.read_mode {
            IoReadMode::Mmap => {
                if let Some(ref cache) = self.mmap_cache {
                    match cache.get_or_map(*segment_id, path) {
                        Ok(mmap) => {
                            let start = file_offset as usize;
                            let end = start.saturating_add(length as usize).min(mmap.len());
                            #[cfg(target_os = "linux")]
                            {
                                // Tell the kernel this is a sequential forward scan
                                // so it can do aggressive read-ahead.
                                let _ = super::segment_reader::madvise_sequential(
                                    mmap.as_ptr(),
                                    mmap.len(),
                                );
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
                                    let _ = super::segment_reader::madvise_dontneed(
                                        mmap.as_ptr(),
                                        mmap.len(),
                                    );
                                }
                            }
                            Ok(data)
                        }
                        Err(e) => Err(format!("mmap read failed for {segment_id}: {e}")),
                    }
                } else {
                    // Mmap mode but no cache configured — fall back to buffered.
                    buffered_read(segment_id, path, file_offset, length).await
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
                        .read(path, &mut buf.as_bytes_mut()[filled..], file_offset + filled as u64)
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
                Ok(Bytes::copy_from_slice(&buf.as_bytes()[..len]))
            }
            IoReadMode::Buffered => buffered_read(segment_id, path, file_offset, length).await,
        }
    }
}

/// Buffered read fallback using `tokio::fs`.
async fn buffered_read(
    segment_id: &SegmentId,
    path: &Path,
    offset: u64,
    length: u32,
) -> std::result::Result<Bytes, String> {
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
    Ok(Bytes::from(buf))
}
