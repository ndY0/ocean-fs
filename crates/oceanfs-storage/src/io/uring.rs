//! Disk I/O backend dispatcher.
//!
//! On Linux 5.1+ with `io_uring` support, `IoBackend::Uring` dispatches
//! I/O via io_uring submission rings. On non-Linux or when the probe
//! fails, `IoBackend::TokioFs` wraps `tokio::fs`.
//!
//! Per performance guideline §3.5.
//!
//! ## io_uring status
//!
//! The `io-uring` feature enables the `Uring` variant. Full integration
//! is deferred — tokio-uring 0.5 changed the `IoUring` API surface and
//! requires migration to the new `Runtime` model. When enabled, the
//! probe always selects `TokioFs` for now.
//!
//! ## Relationship to the [`DiskIo`](crate::io::DiskIo) trait
//!
//! This type is the io module's *concrete* I/O backend. The
//! [`DiskIo`](crate::io::DiskIo) trait (g1 `disk-io-observability`) is
//! the single observed file-op surface the health monitor feeds from;
//! this backend implements it in its default state (pool 0, no-op
//! observer), and the pool-aware [`ObservedIo`](crate::io::ObservedIo)
//! wrapper composes it with a pool id + observer.
// [review][implementation][critical]
// this io performance critical implementation is missing
// [end]
#[cfg(feature = "io-uring")]
use std::path::PathBuf;
use std::{io, path::Path};

use crate::io::direct::TokioOpenOptionsDirectExt;

/// The disk I/O backend.
///
/// Renamed from the pre-g1 `DiskIo` (which the g1 `DiskIo` *trait* now
/// owns): this is the concrete dispatcher, used directly by the segment
/// reader today and composed into the observed surface via
/// [`ObservedIo`](crate::io::ObservedIo).
#[derive(Debug)]
pub enum IoBackend {
    /// Portable fallback using `tokio::fs`. Always available.
    TokioFs,
    /// io_uring backend (Linux only, feature-gated).
    ///
    /// Full integration deferred — tokio-uring 0.5 API migration needed.
    /// Probe always selects `TokioFs` until the worker is migrated.
    #[cfg(feature = "io-uring")]
    Uring,
}

#[cfg(feature = "io-uring")]
#[allow(dead_code)]
enum UringRequest {
    Read {
        path: PathBuf,
        buf_len: usize,
        offset: u64,
        tx: tokio::sync::oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        path: PathBuf,
        data: Vec<u8>,
        tx: tokio::sync::oneshot::Sender<io::Result<()>>,
    },
    Sync {
        path: PathBuf,
        tx: tokio::sync::oneshot::Sender<io::Result<()>>,
    },
    Shutdown,
}

impl IoBackend {
    /// Creates a new instance, probing io_uring availability once.
    pub fn new() -> Self {
        Self::probe()
    }

    /// Reads data from a file at the given offset into `buf`.
    ///
    /// Returns the number of bytes read.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or the read fails.
    pub async fn read(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        match self {
            IoBackend::TokioFs => {
                let mut file = tokio::fs::File::open(path).await?;
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.read(buf).await
            }
            #[cfg(feature = "io-uring")]
            IoBackend::Uring => {
                let mut file = tokio::fs::File::open(path).await?;
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.read(buf).await
            }
        }
    }

    /// Reads data from a file opened with O_DIRECT (bypasses OS page cache).
    ///
    /// Uses aligned I/O via `DirectIoBuf`. On Linux, opens the file with
    /// `O_DIRECT`. Falls back to buffered I/O on non-Linux platforms.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or the read fails.
    pub async fn read_direct(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let mut file = tokio::fs::OpenOptions::new().read(true).with_direct().open(path).await?;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.read(buf).await
    }

    /// Writes `buf` to a file, creating parent directories if needed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write fails or directories cannot be created.
    pub async fn write(&self, path: &Path, buf: &[u8], _offset: u64) -> io::Result<()> {
        match self {
            IoBackend::TokioFs => {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(path, buf).await
            }
            #[cfg(feature = "io-uring")]
            IoBackend::Uring => {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(path, buf).await
            }
        }
    }

    /// Syncs a file to durable storage.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or synced.
    pub async fn sync_file(&self, path: &Path) -> io::Result<()> {
        match self {
            IoBackend::TokioFs => {
                let file = tokio::fs::File::open(path).await?;
                file.sync_all().await
            }
            #[cfg(feature = "io-uring")]
            IoBackend::Uring => {
                let file = tokio::fs::File::open(path).await?;
                file.sync_all().await
            }
        }
    }

    /// Opens a file for reading.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened.
    pub async fn open(&self, path: &Path) -> io::Result<tokio::fs::File> {
        match self {
            IoBackend::TokioFs => tokio::fs::File::open(path).await,
            #[cfg(feature = "io-uring")]
            IoBackend::Uring => tokio::fs::File::open(path).await,
        }
    }

    fn probe() -> Self {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            tracing::info!(
                "io-uring feature enabled; full integration deferred \
                 (tokio-uring 0.5 API migration needed)"
            );
        }
        IoBackend::TokioFs
    }
}

impl Default for IoBackend {
    fn default() -> Self {
        Self::new()
    }
}

/// The io module's concrete [`DiskIo`](crate::io::DiskIo) implementation in its default
/// state: pool 0, [`NoopIoObserver`](crate::io::NoopIoObserver) (no recording). The g1 read path
/// still calls the inherent methods directly; the pool-aware
/// [`ObservedIo`](crate::io::ObservedIo) wrapper composes this backend with a real pool id +
/// observer.
#[async_trait::async_trait]
impl crate::io::DiskIo for IoBackend {
    fn pool_id(&self) -> u32 {
        0
    }

    fn observer(&self) -> &dyn crate::io::IoObserving {
        &NOOP_OBSERVER
    }

    async fn read_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.read(path, buf, offset).await
    }

    async fn read_direct_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.read_direct(path, buf, offset).await
    }

    async fn open_raw(&self, path: &Path) -> io::Result<tokio::fs::File> {
        self.open(path).await
    }

    async fn write_raw(&self, path: &Path, buf: &[u8], offset: u64) -> io::Result<()> {
        self.write(path, buf, offset).await
    }

    async fn fsync_raw(&self, path: &Path) -> io::Result<()> {
        self.sync_file(path).await
    }
}

/// The no-op observer every unattributed [`IoBackend`] records on.
static NOOP_OBSERVER: crate::io::NoopIoObserver = crate::io::NoopIoObserver;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tokio_fs_read_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let io = IoBackend::TokioFs;
        io.write(&path, b"hello world", 0).await.unwrap();
        let mut buf = vec![0u8; 11];
        let n = io.read(&path, &mut buf, 0).await.unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello world");
    }

    #[tokio::test]
    async fn tokio_fs_write_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("nested").join("test.dat");
        let io = IoBackend::TokioFs;
        io.write(&path, b"data", 0).await.unwrap();
        assert!(path.exists());
    }

    #[test]
    fn default_is_tokio_fs() {
        let io = IoBackend::default();
        assert!(matches!(io, IoBackend::TokioFs));
    }
}
