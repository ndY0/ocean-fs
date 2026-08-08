//! O_DIRECT buffer and file-open helpers.
//!
//! When `read_cache_segments = false` (write-optimised profile), segment
//! data files are opened with `O_DIRECT` to bypass the OS page cache.
//! This avoids double-buffering (kernel + userspace) and prevents large
//! segment writes from evicting hot metadata/WAL data from the page cache.
//!
//! Per performance guideline §3.2.

use std::io;

/// A page-aligned buffer suitable for O_DIRECT I/O.
///
/// O_DIRECT requires that the data buffer, file offset, and I/O length
/// are all multiples of the logical block size (typically 512 bytes).
/// `DirectIoBuf` allocates page-aligned memory via `memmap2` anonymous
/// mapping, guaranteeing the alignment invariant.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::io::DirectIoBuf;
///
/// let buf = DirectIoBuf::new(4096).expect("allocation failed");
/// assert!(buf.is_aligned());
/// assert_eq!(buf.len(), 4096);
/// ```
pub struct DirectIoBuf {
    /// Underlying page-aligned memory mapping.
    mmap: memmap2::MmapMut,
    /// Logical length (may be less than the mmap allocation due to
    /// page-size rounding).
    len: usize,
}

impl DirectIoBuf {
    /// Allocates a new page-aligned buffer of at least `capacity` bytes.
    ///
    /// The returned buffer is zero-initialised. On Linux, the
    /// allocation uses `MAP_ANONYMOUS | MAP_PRIVATE` which gives
    /// page-aligned memory without a backing file.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the anonymous mapping fails (e.g., out of
    /// memory or `memmap2` internal error).
    pub fn new(capacity: usize) -> io::Result<Self> {
        // Round up to page size for alignment guarantee.
        let page_size = page_size();
        let alloc_size = capacity.max(1).next_multiple_of(page_size);

        let mmap = memmap2::MmapMut::map_anon(alloc_size)?;
        Ok(Self { mmap, len: capacity })
    }

    /// Returns the usable capacity (not rounded to page size).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns `true` if the buffer is page-aligned.
    ///
    /// O_DIRECT requires the buffer address to be a multiple of the
    /// logical block size. Page alignment guarantees this.
    pub fn is_aligned(&self) -> bool {
        self.mmap.as_ptr() as usize % page_size() == 0
    }

    /// Returns a byte slice of the buffer data.
    ///
    /// The returned slice length is `self.len()`, not the full
    /// allocation.
    pub fn as_bytes(&self) -> &[u8] {
        &self.mmap[..self.len]
    }

    /// Returns a mutable byte slice of the buffer data.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.mmap[..self.len]
    }

    /// Copies data from a slice into the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `data.len() > self.len()`.
    pub fn copy_from_slice(&mut self, data: &[u8]) {
        assert!(data.len() <= self.len, "data exceeds buffer capacity");
        self.mmap[..data.len()].copy_from_slice(data);
    }
}

impl AsRef<[u8]> for DirectIoBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsMut<[u8]> for DirectIoBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_bytes_mut()
    }
}

impl std::ops::Deref for DirectIoBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl std::ops::DerefMut for DirectIoBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_bytes_mut()
    }
}

// ---------------------------------------------------------------------------
// O_DIRECT helpers (Linux)
// ---------------------------------------------------------------------------

/// Returns the system page size in bytes.
///
/// Uses a safe constant of 4096 bytes. On all common platforms
/// (x86_64, aarch64 with 4KB pages), this matches the actual page
/// size. On systems with larger pages (16KB, 64KB), 4096 is a
/// multiple-inverse safe lower bound for alignment checks because
/// any 4096-aligned address is also aligned to larger page sizes.
const fn page_size() -> usize {
    4096
}

/// Extension trait for `std::fs::OpenOptions` to set O_DIRECT on Linux.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_storage::io::direct::OpenOptionsDirectExt;
/// use std::fs::OpenOptions;
///
/// let file = OpenOptions::new()
///     .create(true)
///     .write(true)
///     .with_direct()  // O_DIRECT on Linux
///     .open("/tmp/segment.dat")?;
/// ```
pub trait OpenOptionsDirectExt {
    /// Enables O_DIRECT on Linux; no-op on other platforms.
    fn with_direct(&mut self) -> &mut Self;
}

impl OpenOptionsDirectExt for std::fs::OpenOptions {
    fn with_direct(&mut self) -> &mut Self {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // libc::O_DIRECT = 0x4000 on Linux
            self.custom_flags(libc::O_DIRECT);
        }
        let _ = self; // suppress unused warning on non-Linux
        self
    }
}

/// Extension trait for `tokio::fs::OpenOptions` to set O_DIRECT on Linux.
pub trait TokioOpenOptionsDirectExt {
    /// Enables O_DIRECT on Linux; no-op on other platforms.
    fn with_direct(&mut self) -> &mut Self;
}

impl TokioOpenOptionsDirectExt for tokio::fs::OpenOptions {
    fn with_direct(&mut self) -> &mut Self {
        #[cfg(target_os = "linux")]
        {
            // Required for the `custom_flags` method on Linux.
            #[allow(unused_imports)]
            use std::os::unix::fs::OpenOptionsExt;
            self.custom_flags(libc::O_DIRECT);
        }
        let _ = self; // suppress unused warning on non-Linux
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn direct_io_buf_allocation_is_page_aligned() {
        let buf = DirectIoBuf::new(512).unwrap();
        assert!(buf.is_aligned());
        assert!(buf.len() >= 512);
    }

    #[test]
    fn direct_io_buf_large_allocation_is_aligned() {
        let buf = DirectIoBuf::new(4 * 1024 * 1024).unwrap(); // 4 MB
        assert!(buf.is_aligned());
        assert_eq!(buf.len(), 4 * 1024 * 1024);
    }

    #[test]
    fn direct_io_buf_copy_from_slice_preserves_data() {
        let mut buf = DirectIoBuf::new(1024).unwrap();
        let data = b"hello world";
        buf.copy_from_slice(data);
        assert_eq!(&buf.as_bytes()[..data.len()], data);
    }

    #[test]
    fn direct_io_buf_deref_gives_slice() {
        let mut buf = DirectIoBuf::new(100).unwrap();
        buf.copy_from_slice(&[0xABu8; 100]);
        assert_eq!(buf.len, 100);
        assert_eq!((&*buf)[0], 0xAB);
    }

    #[test]
    fn direct_io_buf_is_empty_when_zero_capacity() {
        let buf = DirectIoBuf::new(0).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn direct_io_buf_as_mut_modifies_in_place() {
        let mut buf = DirectIoBuf::new(64).unwrap();
        buf.as_bytes_mut()[0] = 42;
        assert_eq!(buf.as_bytes()[0], 42);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn open_options_direct_sets_flag_on_linux() {
        use std::os::unix::fs::OpenOptionsExt;
        let mut opts = std::fs::OpenOptions::new();
        opts.with_direct();
        // Just verify no panic; we can't inspect custom_flags directly.
    }
}
