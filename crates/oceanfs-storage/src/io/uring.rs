//! Disk I/O backend dispatcher.
//!
//! On Linux 5.1+ with `io_uring` support, `DiskIo::Uring` dispatches
//! I/O via io_uring submission rings. On non-Linux or when the probe
//! fails, `DiskIo::TokioFs` wraps `tokio::fs`.
//!
//! Per performance guideline §3.5.
//!
//! ## io_uring status
//!
//! The `io-uring` feature enables the `Uring` variant. Full integration
//! is deferred — tokio-uring 0.5 changed the `IoUring` API surface and
//! requires migration to the new `Runtime` model. When enabled, the
//! probe always selects `TokioFs` for now.

#[cfg(feature = "io-uring")]
use std::path::PathBuf;
use std::{io, path::Path};

/// The disk I/O backend.
pub enum DiskIo {
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

impl DiskIo {
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
            DiskIo::TokioFs => {
                let mut file = tokio::fs::File::open(path).await?;
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.read(buf).await
            }
            #[cfg(feature = "io-uring")]
            DiskIo::Uring => {
                let mut file = tokio::fs::File::open(path).await?;
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.read(buf).await
            }
        }
    }

    /// Writes `buf` to a file, creating parent directories if needed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write fails or directories cannot be created.
    pub async fn write(&self, path: &Path, buf: &[u8], _offset: u64) -> io::Result<()> {
        match self {
            DiskIo::TokioFs => {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(path, buf).await
            }
            #[cfg(feature = "io-uring")]
            DiskIo::Uring => {
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
            DiskIo::TokioFs => {
                let file = tokio::fs::File::open(path).await?;
                file.sync_all().await
            }
            #[cfg(feature = "io-uring")]
            DiskIo::Uring => {
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
            DiskIo::TokioFs => tokio::fs::File::open(path).await,
            #[cfg(feature = "io-uring")]
            DiskIo::Uring => tokio::fs::File::open(path).await,
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
        DiskIo::TokioFs
    }
}

impl Default for DiskIo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tokio_fs_read_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let io = DiskIo::TokioFs;
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
        let io = DiskIo::TokioFs;
        io.write(&path, b"data", 0).await.unwrap();
        assert!(path.exists());
    }

    #[test]
    fn default_is_tokio_fs() {
        let io = DiskIo::default();
        assert!(matches!(io, DiskIo::TokioFs));
    }
}
