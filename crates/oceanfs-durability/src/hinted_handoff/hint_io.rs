//! Hint-WAL I/O observability seam.
//!
//! The hint WAL performs raw `std::fs` I/O ([`crate::HintWal`]), which the
//! storage [`IoObserver`](oceanfs_storage::io::IoObserver) never sees — so a
//! device that fails only under hint writes stays invisible to pool health.
//! [`HintIoRecorder`] is the thin seam that closes that gap: the manager
//! times each `HintWal::open` / `HintWal::write_hint` call and records the
//! outcome into the node's observer under the hints pool id, using exactly
//! the same signal surface as the observed data path.
//!
//! Together with the periodic [`PoolRootProbe`](oceanfs_storage::PoolRootProbe)
//! this gives the hints pool a health producer even when it is idle.

use std::{sync::Arc, time::Duration};

use oceanfs_storage::io::{IoErrorKind, IoObserving, IoOp};

use crate::error::Error;

/// Records hint-WAL I/O outcomes into the shared storage I/O observer.
///
/// The recorder carries the hints pool id because the WAL itself has no
/// pool context (its directory is resolved by the composition root).
/// Cloning shares the observer.
///
/// # Examples
///
/// ```ignore
/// // Requires a node-style IoObserver registered for the hints pool:
/// let recorder = HintIoRecorder::new(observer, hints_pool_id);
/// let manager = HintedHandoffManager::new(dir, client, config)
///     .with_io_recorder(recorder);
/// ```
#[derive(Clone, Debug)]
pub struct HintIoRecorder {
    observer: Arc<dyn IoObserving>,
    pool_id: u32,
}

impl HintIoRecorder {
    /// Creates a recorder bound to one pool id.
    ///
    /// `observer` is the node's shared observer (the same instance the
    /// health monitor snapshots); `pool_id` must be registered with it.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_durability::HintIoRecorder;
    /// use oceanfs_storage::IoObserver;
    ///
    /// let observer = Arc::new(IoObserver::new());
    /// observer.register_pool(3, None);
    /// let recorder = HintIoRecorder::new(observer, 3);
    /// # let _ = recorder;
    /// ```
    pub fn new(observer: Arc<dyn IoObserving>, pool_id: u32) -> Self {
        Self { observer, pool_id }
    }

    /// Records one WAL operation: latency always, plus the mapped error kind
    /// when the operation failed.
    ///
    /// Mirrors the observed-I/O wrappers: a failing operation still
    /// contributes its latency so the trend sees the attempt.
    pub(crate) fn record(&self, op: IoOp, duration: Duration, error: Option<&Error>) {
        self.observer.record_latency(self.pool_id, op, duration);
        if let Some(error) = error {
            self.observer.record_error(self.pool_id, error_kind(error));
        }
    }
}

/// Maps a durability error to the storage observer's error-kind space.
///
/// Non-I/O failures (directory creation wrapped as `Internal`, record
/// encoding) map to `Other`, which still feeds the error trend and the
/// confirmed-loss classifier ("other" is the unplug/EIO bucket).
fn error_kind(error: &Error) -> IoErrorKind {
    match error {
        Error::Io(io) => IoErrorKind::from_io_error(io),
        _ => IoErrorKind::Other,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn error_kind_maps_io_storage_full() {
        let error = Error::Io(io::Error::from(io::ErrorKind::StorageFull));
        assert_eq!(error_kind(&error), IoErrorKind::StorageFull);
    }

    #[test]
    fn error_kind_maps_non_io_to_other() {
        let error = Error::Internal("directory create failed".into());
        assert_eq!(error_kind(&error), IoErrorKind::Other);
    }
}
