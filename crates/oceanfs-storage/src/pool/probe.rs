//! Periodic pool-root write probe.
//!
//! Pool health is derived from *observed* I/O signals (ADR-0029 §D3): the
//! health monitor only ever transitions a pool on evidence recorded through
//! [`IoObserver`]. An **idle** pool therefore produces no signals at all —
//! a dead or full device behind a mounted-but-idle root (the hints pool in
//! particular) can stay `Healthy` indefinitely because nothing writes to it
//! to observe the failure.
//!
//! [`PoolRootProbe`] is the producer for that case: a cheap, idempotent
//! create+write+fsync cycle against the pool root, timed and error-classified
//! through the same [`IoObserver`] the data path uses. Each failing cycle
//! contributes observed errors/kinds, so the existing health monitor drives
//! `Healthy → Degraded → Dead` with no new state machine.
//!
//! The probe deliberately does **not** create the root: a missing root must
//! surface as a probe error (NotFound/NotADirectory), not be silently
//! recreated on the wrong filesystem (e.g. the mountpoint directory left
//! behind by a detached volume would otherwise absorb the writes).
//!
//! Per performance guideline §7.1 the probe performs no work under a lock;
//! per §3.1 the write is a single small append-style write and one fsync.

use std::{io::Write, path::PathBuf, sync::Arc, time::Instant};

use crate::io::{IoErrorKind, IoObserver, IoOp};

/// Filename of the probe file. Fixed (not per-run unique): the file is
/// truncated on every cycle and removed after, so a crashed probe leaves at
/// most one stale file that the next cycle reuses instead of accumulating
/// litter on the root.
const PROBE_FILENAME: &str = ".oceanfs-pool-probe";

/// Payload written by each probe cycle: small, constant, and non-empty so the
/// write path (not just open) is exercised.
const PROBE_PAYLOAD: &[u8] = b"oceanfs pool probe";

/// A periodic write probe for one pool root.
///
/// The probe records its operations into the node's shared
/// [`IoObserver`] under the pool's id, which is what the health monitor
/// consumes. It holds no state beyond the root/pool binding, so a single
/// probe instance can run cycle after cycle.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_storage::{IoObserver, PoolRootProbe};
///
/// let observer = Arc::new(IoObserver::new());
/// observer.register_pool(0, None);
/// let probe = PoolRootProbe::new(0, std::env::temp_dir(), observer.clone());
/// probe.run_once().expect("probe cycle");
/// let signal = observer.snapshot(0).expect("registered pool");
/// assert!(signal.ops >= 3, "open + write + fsync are observed");
/// ```
#[derive(Clone, Debug)]
pub struct PoolRootProbe {
    pool_id: u32,
    root: PathBuf,
    observer: Arc<IoObserver>,
}

impl PoolRootProbe {
    /// Binds a probe to one pool root.
    ///
    /// `pool_id` must be registered with `observer` (the pool registry does
    /// this via `observe_into` at construction); an unregistered id simply
    /// records into a slot nobody reads.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_storage::{IoObserver, PoolRootProbe};
    ///
    /// let observer = Arc::new(IoObserver::new());
    /// observer.register_pool(2, None);
    /// let probe = PoolRootProbe::new(2, std::env::temp_dir(), observer);
    /// # let _ = probe;
    /// ```
    pub fn new(pool_id: u32, root: impl Into<PathBuf>, observer: Arc<IoObserver>) -> Self {
        Self { pool_id, root: root.into(), observer }
    }

    /// Runs one probe cycle: create/truncate → write → fsync → remove.
    ///
    /// Every operation is timed; failures additionally record their
    /// [`IoErrorKind`] (so unplug-class errors feed the confirmed-loss path
    /// and `StorageFull` feeds the Degraded trend). The probe file is removed
    /// on success and best-effort on failure.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error when the root is missing, read-only,
    /// full, or the device errors — the caller logs it; the health signal is
    /// the durable record.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_storage::{IoObserver, PoolRootProbe};
    ///
    /// let observer = Arc::new(IoObserver::new());
    /// observer.register_pool(2, None);
    /// let probe = PoolRootProbe::new(2, std::env::temp_dir(), observer);
    /// probe.run_once().expect("temp dir is writable");
    /// ```
    pub fn run_once(&self) -> std::io::Result<()> {
        let probe_path = self.root.join(PROBE_FILENAME);

        let started = Instant::now();
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&probe_path)
        {
            Ok(file) => {
                self.observer.record_latency(self.pool_id, IoOp::Open, started.elapsed());
                file
            }
            Err(error) => {
                self.record_failure(IoOp::Open, started, &error);
                return Err(error);
            }
        };

        let started = Instant::now();
        if let Err(error) = file.write_all(PROBE_PAYLOAD) {
            self.record_failure(IoOp::Write, started, &error);
            let _ = std::fs::remove_file(&probe_path);
            return Err(error);
        }
        self.observer.record_latency(self.pool_id, IoOp::Write, started.elapsed());

        let started = Instant::now();
        if let Err(error) = file.sync_all() {
            self.record_failure(IoOp::Fsync, started, &error);
            let _ = std::fs::remove_file(&probe_path);
            return Err(error);
        }
        self.observer.record_latency(self.pool_id, IoOp::Fsync, started.elapsed());

        drop(file);
        let _ = std::fs::remove_file(&probe_path);
        Ok(())
    }

    /// Records a failed operation: latency always (mirroring the observed
    /// I/O wrappers), plus the mapped error kind.
    fn record_failure(&self, op: IoOp, started: Instant, error: &std::io::Error) {
        self.observer.record_latency(self.pool_id, op, started.elapsed());
        self.observer.record_error(self.pool_id, IoErrorKind::from_io_error(error));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn probe_success_records_open_write_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let observer = Arc::new(IoObserver::new());
        observer.register_pool(7, None);
        let probe = PoolRootProbe::new(7, dir.path(), observer.clone());

        probe.run_once().expect("probe cycle on a writable root");

        let signal = observer.snapshot(7).expect("pool is registered");
        assert_eq!(signal.errors, 0, "a healthy root produces no errors");
        assert!(signal.ops >= 3, "open + write + fsync are recorded: {}", signal.ops);
    }

    #[test]
    fn probe_is_clean_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let observer = Arc::new(IoObserver::new());
        observer.register_pool(1, None);
        let probe = PoolRootProbe::new(1, dir.path(), observer);

        probe.run_once().unwrap();
        probe.run_once().unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(entries.is_empty(), "probe file must not linger: {entries:?}");
    }

    #[test]
    fn probe_missing_root_records_not_found_and_fails() {
        let dir = tempfile::tempdir().unwrap();
        let observer = Arc::new(IoObserver::new());
        observer.register_pool(3, None);
        let missing = dir.path().join("not-mounted");
        let probe = PoolRootProbe::new(3, &missing, observer.clone());

        let error = probe.run_once().expect_err("missing root must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(observer.io_error_count(3), 1, "the failure is observed");
        let signal = observer.snapshot(3).expect("pool is registered");
        assert_eq!(
            signal.error_kinds[IoErrorKind::NotFound as usize],
            1,
            "the error kind feeds the confirmed-loss classifier"
        );
    }

    #[test]
    fn probe_path_through_file_records_not_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let file_root = dir.path().join("regular-file");
        std::fs::write(&file_root, b"not a directory").unwrap();
        let observer = Arc::new(IoObserver::new());
        observer.register_pool(4, None);
        let probe = PoolRootProbe::new(4, &file_root, observer.clone());

        let error = probe.run_once().expect_err("non-directory root must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::NotADirectory);
        let signal = observer.snapshot(4).expect("pool is registered");
        assert_eq!(signal.error_kinds[IoErrorKind::NotADirectory as usize], 1);
    }
}
