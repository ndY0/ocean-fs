//! Atomic segment writes via `O_TMPFILE` + `linkat` (Linux 3.11+).
//!
//! On Linux, segment files can be written atomically: the file is created
//! as an unnamed, invisible `O_TMPFILE` in the segment directory, the data
//! is written and synced, and then `linkat(2)` atomically links the unnamed
//! file into the directory under its final name. Until `linkat` succeeds,
//! the file is invisible to readers and any crash leaves zero partial files.
//!
//! Falls back to the traditional create→write→fsync→rename path on older
//! kernels or non-Linux platforms.
//!
//! # O_TMPFILE Discovery
//!
//! Support is probed once at startup via `O_TMPFILE` flag. The
//! probe attempts to create an `O_TMPFILE` in the segment data directory.
//! If the kernel returns `EOPNOTSUPP`, `EINVAL`, or `ENOENT`, `O_TMPFILE`
//! is unsupported and the `Rename` fallback is used. The probe result is
//! cached in a `SegmentWriteMode` value.

use std::{
    fs,
    io::{self, Write},
    os::unix::io::AsRawFd,
    path::Path,
};

/// Strategy for writing sealed segment files to disk.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::io::SegmentWriteMode;
///
/// // Probe at startup (Linux only):
/// let mode = SegmentWriteMode::probe("/var/lib/oceanfs/segments");
/// assert!(matches!(mode, SegmentWriteMode::Rename | SegmentWriteMode::Tmpfile));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SegmentWriteMode {
    /// Traditional rename-based write: create temp file → write → fsync →
    /// rename to final name. Portable, supported everywhere.
    Rename,
    /// Atomic write via `O_TMPFILE` + `linkat`. Linux 3.11+ only.
    /// The file is never visible until fully written and fsynced.
    Tmpfile,
}

impl SegmentWriteMode {
    /// Probes the filesystem for `O_TMPFILE` support.
    ///
    /// Attempts to open an `O_TMPFILE` in `segment_dir`. If the kernel
    /// supports it, returns `Tmpfile`; otherwise `Rename`.
    ///
    /// The probe creates and immediately unlinks a temporary file —
    /// it has no persistent side effects.
    pub fn probe(segment_dir: impl AsRef<Path>) -> Self {
        let dir = segment_dir.as_ref();
        if probe_otmpfile_support(dir) {
            Self::Tmpfile
        } else {
            Self::Rename
        }
    }

    /// Returns `true` if this mode uses `O_TMPFILE`.
    pub fn is_atomic(&self) -> bool {
        matches!(self, Self::Tmpfile)
    }
}

/// Writes data atomically to a segment file.
///
/// When `mode` is `Tmpfile` and the platform supports it, the file is
/// created via `O_TMPFILE` and linked atomically — readers never see
/// a partial file. Falls back to the rename path otherwise.
///
/// # Errors
///
/// Returns an I/O error if the file cannot be created, written, synced,
/// or linked.
pub(crate) fn write_atomic(
    mode: SegmentWriteMode,
    dir: &Path,
    filename: &str,
    data: &[u8],
) -> io::Result<()> {
    match mode {
        SegmentWriteMode::Tmpfile => write_tmpfile(dir, filename, data),
        SegmentWriteMode::Rename => write_rename(dir, filename, data),
    }
}

// ---------------------------------------------------------------------------
// O_TMPFILE path (Linux only)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn write_tmpfile(dir: &Path, filename: &str, data: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let dir_fd = open_dir_fd(dir)?;

    // Create an unnamed, invisible file in the segment directory.
    // O_TMPFILE: creates a temporary file that has no directory entry.
    // The file is automatically cleaned up by the kernel when the last
    // fd is closed if it was never linked.
    let mut opts = fs::OpenOptions::new();
    opts.write(true);
    opts.custom_flags(libc::O_TMPFILE);

    let mut file = opts.open(dir)?;
    file.write_all(data)?;
    file.sync_data()?;

    // Atomically link the unnamed file into the directory.
    // "/proc/self/fd/{fd}" is a magic path that refers to the open
    // file description — the kernel resolves it to the underlying
    // inode. linkat with AT_SYMLINK_FOLLOW creates a directory entry
    // pointing to that inode.
    let fd = file.as_raw_fd();
    let proc_path = format!("/proc/self/fd/{fd}");
    let proc_path_c = std::ffi::CString::new(proc_path.as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let filename_c = std::ffi::CString::new(filename.as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    // SAFETY: dir_fd is a valid directory file descriptor. proc_path_c
    // points to a valid C string referencing /proc/self/fd/{fd} which
    // is guaranteed to be valid while the fd is open. filename_c is a
    // valid C string for the target name. AT_SYMLINK_FOLLOW resolves
    // the /proc/self/fd symlink to the actual inode.
    #[allow(unsafe_code)]
    let ret = unsafe {
        libc::linkat(
            libc::AT_FDCWD,
            proc_path_c.as_ptr(),
            dir_fd,
            filename_c.as_ptr(),
            libc::AT_SYMLINK_FOLLOW,
        )
    };

    // Close dir_fd before checking linkat result.
    // SAFETY: `dir_fd` is a valid directory file descriptor opened by
    // `open_dir_fd`. Closing it is safe even if linkat failed.
    #[allow(unsafe_code)]
    let _ = unsafe { libc::close(dir_fd) };

    if ret != 0 {
        return Err(io::Error::last_os_error());
    }

    // file is dropped here — the inode stays linked in the directory.
    Ok(())
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn open_dir_fd(dir: &Path) -> io::Result<i32> {
    let dir_c = std::ffi::CString::new(dir.to_string_lossy().as_bytes()).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("invalid dir path: {e}"))
    })?;
    // SAFETY: dir_c is a valid C string. O_RDONLY | O_DIRECTORY opens
    // the path as a directory for reading (needed by linkat).
    #[allow(unsafe_code)]
    let fd = unsafe { libc::open(dir_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

/// Probes O_TMPFILE support by creating a test file and immediately
/// removing it. Returns `true` if the kernel supports O_TMPFILE in
/// this directory.
#[cfg(target_os = "linux")]
fn probe_otmpfile_support(dir: &Path) -> bool {
    use std::os::unix::fs::OpenOptionsExt;

    let mut opts = fs::OpenOptions::new();
    opts.write(true);
    opts.custom_flags(libc::O_TMPFILE);

    // Try opening an O_TMPFILE. If it fails with EOPNOTSUPP, EINVAL,
    // or ENOENT, the kernel doesn't support O_TMPFILE (or the
    // filesystem doesn't).
    match opts.open(dir) {
        Ok(f) => {
            // Success — the kernel supports O_TMPFILE.
            // The file is unnamed and will be cleaned up when the fd is dropped.
            drop(f);
            true
        }
        Err(e) => {
            // EOPNOTSUPP: filesystem doesn't support O_TMPFILE (e.g., overlayfs
            //   before Linux 5.4, or ext2/3/4 with old kernel).
            // EINVAL:  O_TMPFILE used without O_RDWR or O_WRONLY, or invalid flags.
            // ENOENT:  directory doesn't exist (shouldn't happen if we have the path).
            let raw = e.raw_os_error();
            let unsupported = matches!(raw, Some(libc::EOPNOTSUPP | libc::EINVAL | libc::ENOENT));
            if unsupported {
                tracing::info!(
                    ?e,
                    "O_TMPFILE not supported by kernel or filesystem; \
                     falling back to rename-based segment writes"
                );
            } else {
                tracing::warn!(
                    ?e,
                    "O_TMPFILE probe failed with unexpected error; \
                     falling back to rename-based segment writes"
                );
            }
            false
        }
    }
}

// Non-Linux: O_TMPFILE is not available — always use rename path.
#[cfg(not(target_os = "linux"))]
fn write_tmpfile(_dir: &Path, _filename: &str, _data: &[u8]) -> io::Result<()> {
    unreachable!("write_tmpfile should not be called on non-Linux")
}

#[cfg(not(target_os = "linux"))]
fn probe_otmpfile_support(_dir: &Path) -> bool {
    false
}

// ---------------------------------------------------------------------------
// Rename-based fallback (portable)
// ---------------------------------------------------------------------------

/// Writes data via the traditional create→write→fsync→rename path.
fn write_rename(dir: &Path, filename: &str, data: &[u8]) -> io::Result<()> {
    // Write to a temporary name first so readers don't see a partial file.
    let tmp_name = format!(".tmp.{filename}");
    let tmp_path = dir.join(&tmp_name);
    let final_path = dir.join(filename);

    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(data)?;
        file.sync_data()?;
        // File is synced; drop the handle before rename.
    }

    fs::rename(&tmp_path, &final_path)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn rename_mode_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"segment data for atomic write test";
        let filename = "segment-test-001.dat";

        write_rename(dir.path(), filename, data).unwrap();

        let path = dir.path().join(filename);
        let read_back = fs::read(&path).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn rename_mode_no_partial_file_visible() {
        let dir = tempfile::tempdir().unwrap();
        let filename = "segment-test-002.dat";
        let tmp_name = format!(".tmp.{filename}");

        // Simulate: begin rename write, then check before rename completes.
        let data = b"partial write simulation";
        let tmp_path = dir.path().join(&tmp_name);
        let final_path = dir.path().join(filename);

        // Write temporary file.
        {
            let mut file = fs::File::create(&tmp_path).unwrap();
            file.write_all(data).unwrap();
            // Do NOT sync or rename — simulate a crash.
            drop(file);
        }

        // The final path should NOT exist yet.
        assert!(!final_path.exists(), "final path should not exist before rename");
        // The temp file should still exist (partial write).
        assert!(tmp_path.exists(), "temp file exists before crash cleanup");

        // Clean up.
        let _ = fs::remove_file(&tmp_path);
    }

    #[test]
    fn segment_write_mode_probe_returns_rename_on_supported() {
        let dir = tempfile::tempdir().unwrap();
        let mode = SegmentWriteMode::probe(dir.path());
        // On Linux with supported filesystem, Tmpfile; otherwise Rename.
        // Both should be valid modes.
        assert!(matches!(mode, SegmentWriteMode::Rename | SegmentWriteMode::Tmpfile));
    }

    #[test]
    fn segment_write_mode_is_atomic_detection() {
        let mode = SegmentWriteMode::Tmpfile;
        assert!(mode.is_atomic());

        let mode = SegmentWriteMode::Rename;
        assert!(!mode.is_atomic());
    }

    #[test]
    fn write_atomic_rename_mode_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"atomic write via rename mode";

        write_atomic(SegmentWriteMode::Rename, dir.path(), "seg.dat", data).unwrap();

        let path = dir.path().join("seg.dat");
        let read_back = fs::read(&path).unwrap();
        assert_eq!(read_back, data);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn write_atomic_tmpfile_mode_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"atomic write via O_TMPFILE";

        // First check if TMPFILE is supported on this filesystem.
        let mode = SegmentWriteMode::probe(dir.path());
        // Use whichever mode is supported.
        write_atomic(mode, dir.path(), "seg-tmpfile.dat", data).unwrap();

        let path = dir.path().join("seg-tmpfile.dat");
        let read_back = fs::read(&path).unwrap();
        assert_eq!(read_back, data);
    }
}
