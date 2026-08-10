//! Platform I/O optimisation backend.
//!
//! This module provides the four I/O performance paths from the platform
//! I/O optimisations feature (§3.2-3.6):
//!
//! - **O_DIRECT** — bypass OS page cache for segment data files
//!   (write-optimised profile, `read_cache_segments = false`).
//! - **mmap** — zero-copy reads from the kernel page cache
//!   (read-optimised profile, `read_cache_segments = true`).
//! - **io_uring** — true async disk I/O on Linux 5.1+
//!   (`#[cfg(feature = "io-uring")]`).
//! - **sendfile/splice** — kernel-space copy from disk to network socket
//!   (`#[cfg(feature = "sendfile")]`).
//!
//! All platform-specific code has portable `tokio::fs` fallbacks per
//! performance guideline §10.6.
//!
//! ## I/O Strategy Selection
//!
//! The read strategy is resolved from `NodeConfig::read_cache_segments`:
//!
//! | `read_cache_segments` | Read mode | Write mode |
//! |---|---|---|
//! | `false` (default) | Buffered `tokio::fs::read` | O_DIRECT |
//! | `true`  | mmap | Buffered |
//!
//! The `io_uring` backend is selected at startup via `DiskIo::new()` which
//! probes `io_uring` availability once and caches the result. When available,
//! I/O requests are dispatched to a dedicated background thread running an
//! `io_uring` event loop — fully asynchronous, no thread-pool contention.

pub mod atomic_write;
pub mod direct;
pub mod mmap;
pub mod sched;
pub mod segment_reader;
#[cfg(feature = "sendfile")]
pub mod sendfile;
pub mod uring;

pub(crate) use atomic_write::write_atomic;
pub use atomic_write::SegmentWriteMode;
pub use direct::DirectIoBuf;
pub use mmap::SegmentFileCache;
pub use sched::{apply_background_cpu_sched, apply_background_io_class};
pub use segment_reader::{
    DiskSegmentReader, InMemorySegmentReader, PoolFallbackReader, SegmentReadSource, SegmentReader,
};
#[cfg(feature = "sendfile")]
pub use sendfile::SegmentFileBody;
pub use uring::DiskIo;

/// Read strategy resolved from configuration.
///
/// Determines how segment data is read from disk.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::io::IoReadMode;
///
/// let mode = IoReadMode::from_config(false);
/// assert!(matches!(mode, IoReadMode::Direct));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IoReadMode {
    /// Bypass the OS page cache — use `O_DIRECT`.
    ///
    /// Best for write-optimised workloads where segment data is
    /// rarely re-read.
    Direct,
    /// Standard buffered I/O via `tokio::fs`.
    ///
    /// Portable fallback for non-Linux or when neither Direct
    /// nor Mmap is selected.
    Buffered,
    /// Memory-map segment files for zero-copy reads.
    ///
    /// Best for read-optimised workloads where frequently-accessed
    /// segments benefit from the kernel page cache.
    Mmap,
}

impl IoReadMode {
    /// Resolves the I/O read mode from the `read_cache_segments` config flag.
    ///
    /// On non-Linux platforms, `Mmap` degrades to `Buffered`
    /// (mmap is available but the feature path prefers safety).
    /// `Direct` always degrades to `Buffered` on non-Linux.
    pub fn from_config(read_cache_segments: bool) -> Self {
        if read_cache_segments {
            IoReadMode::Mmap
        } else {
            IoReadMode::Direct
        }
    }
}
