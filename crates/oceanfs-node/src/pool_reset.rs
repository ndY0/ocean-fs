//! Dead-pool runtime recovery hook (pr2, pool-runtime-lifecycle).
//!
//! The admin route `POST /admin/pools/{id}/reset` delegates here: a
//! probe-gated return of a Dead data pool plus the return-residue sweep
//! that keeps f5's reconciliation accounting honest.

use std::{collections::HashSet, sync::Arc};

use oceanfs_core::{NodeId, SegmentId};
use oceanfs_server::admin::{PoolResetError, PoolResetOutcome};
use oceanfs_storage::{
    pool::health::HealthMonitor,
    segment::lifecycle::{SegmentLifecycleCoordinator, SegmentState},
    PoolRegistry, PoolStatus,
};
use oceanfs_storage_api::SegmentDataStore;
use tracing::{info, warn};

/// Probe-gated runtime return of a Dead data pool (pr2).
///
/// 1. [`PoolRegistry::reset_dead_pool`] re-probes the root and, on success,
///    sets the pool `Healthy`, clears `write_degraded`, and refreshes its
///    capacity. A probe failure returns [`PoolResetError::ProbeFailed`] and
///    the pool stays `Dead`.
/// 2. `HealthMonitor::reset_pool` clears the monitor's internal Dead latch
///    (the same pairing the g7/g8 recovery paths use).
/// 3. The return-residue sweep removes this node from the
///    `storage_locations` of every Sealed registry entry naming the pool
///    whose local `.dat` is absent, through the durable metadata-refresh
///    path, so reconciliation (f5) sees the corrected holder set and
///    repairs the lost copies. Present files are left untouched.
///
/// The caller rebuilds and re-declares the manifest after this returns
/// (the pool's status and capacity changed).
pub(crate) async fn reset_dead_pool(
    registry: &Arc<PoolRegistry>,
    health_monitor: &HealthMonitor,
    lifecycle: &Arc<SegmentLifecycleCoordinator>,
    data_store: &Arc<dyn SegmentDataStore>,
    self_id: &NodeId,
    pool_id: u32,
) -> Result<PoolResetOutcome, PoolResetError> {
    registry.reset_dead_pool(pool_id).map_err(|error| match error {
        oceanfs_storage::PoolResetError::UnknownPool(id) => PoolResetError::UnknownPool(id),
        oceanfs_storage::PoolResetError::NotDataPool(id) => PoolResetError::NotDataPool(id),
        oceanfs_storage::PoolResetError::NotDead(id) => PoolResetError::NotDead(id),
        oceanfs_storage::PoolResetError::ProbeFailed(reason) => PoolResetError::ProbeFailed(reason),
    })?;
    health_monitor.reset_pool(pool_id, PoolStatus::Healthy);

    let (segments_released, sweep_failures) =
        sweep_missing_segments(registry, lifecycle, data_store, self_id, pool_id).await;
    info!(
        pool_id,
        segments_released, sweep_failures, "dead data pool returned at runtime; residue sweep done"
    );
    Ok(PoolResetOutcome { pool_id, status: "healthy", segments_released, sweep_failures })
}

/// Removes `self_id` from the `storage_locations` of Sealed entries that
/// name `pool_id` but whose local `.dat` file is absent; entries whose
/// file is present are kept. Returns `(released, failures)`.
///
/// The listing comes from the store (`list_segment_files`), the same
/// layout source the boot residue sweep uses — never path guessing.
async fn sweep_missing_segments(
    registry: &Arc<PoolRegistry>,
    lifecycle: &Arc<SegmentLifecycleCoordinator>,
    data_store: &Arc<dyn SegmentDataStore>,
    self_id: &NodeId,
    pool_id: u32,
) -> (usize, usize) {
    let Some(pool) = registry.pool_by_id(pool_id) else {
        return (0, 0);
    };
    let present: HashSet<SegmentId> = match data_store.list_segment_files(pool.root()) {
        Ok(listed) => listed.iter().filter_map(|path| parse_segment_file(path)).collect(),
        Err(error) => {
            warn!(
                pool_id,
                root = ?pool.root(),
                error = %error,
                "return-residue listing failed; leaving storage_locations untouched"
            );
            return (0, 0);
        }
    };

    // Snapshot the stale entries before the async durable writes (the
    // registry shard lock is never held across I/O).
    let mut stale: Vec<(SegmentId, smallvec::SmallVec<[NodeId; 16]>)> = Vec::new();
    lifecycle.registry().for_each(|segment_id, entry| {
        if entry.metadata.pool_id != pool_id || entry.state != SegmentState::Sealed {
            return;
        }
        if present.contains(&segment_id) {
            return;
        }
        if !entry.metadata.storage_locations.contains(self_id) {
            return;
        }
        let mut locations = entry.metadata.storage_locations.clone();
        locations.retain(|node| node != self_id);
        stale.push((segment_id, locations));
    });

    let mut released = 0;
    let mut failures = 0;
    for (segment_id, locations) in stale {
        match lifecycle.persist_storage_locations(segment_id, locations).await {
            Ok(()) => released += 1,
            Err(error) => {
                failures += 1;
                warn!(
                    segment_id = %segment_id,
                    pool_id,
                    error = %error,
                    "return-residue storage_locations refresh failed (drift scan retries)"
                );
            }
        }
    }
    (released, failures)
}

/// Parses `{segment_id}.dat` file names into segment ids.
fn parse_segment_file(path: &std::path::Path) -> Option<SegmentId> {
    let name = path.file_name()?.to_str()?;
    let id = name.strip_suffix(".dat")?;
    let uuid = uuid::Uuid::parse_str(id).ok()?;
    Some(SegmentId::from_uuid_bytes(*uuid.as_bytes()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_storage_api::error::Error as StorageApiError;

    use super::*;

    /// A store whose root listing always fails.
    struct FailingListingStore;

    #[async_trait::async_trait]
    impl SegmentDataStore for FailingListingStore {
        async fn write_segment_data(
            &self,
            _segment_id: &SegmentId,
            _data: &[u8],
        ) -> Result<(), StorageApiError> {
            Ok(())
        }

        async fn read_segment_data(
            &self,
            _segment_id: &SegmentId,
        ) -> Result<Option<oceanfs_storage_api::SegmentFile>, StorageApiError> {
            Ok(None)
        }

        async fn delete_shards(&self, _segment_id: &SegmentId) -> Result<u64, StorageApiError> {
            Ok(0)
        }

        async fn delete_shards_with_pool(
            &self,
            _segment_id: &SegmentId,
            _pool_id: u32,
        ) -> Result<u64, StorageApiError> {
            Ok(0)
        }

        fn list_segment_files(
            &self,
            _root: &std::path::Path,
        ) -> Result<Vec<std::path::PathBuf>, StorageApiError> {
            Err(StorageApiError::Io(std::io::Error::other("listing failed")))
        }
    }

    fn test_registry(tmp: &std::path::Path) -> Arc<PoolRegistry> {
        let pool = |name: &str, role: oceanfs_core::PoolRole| oceanfs_core::StoragePoolConfig {
            name: name.into(),
            role,
            root: tmp.join(name),
            weight: None,
            tech: Default::default(),
            health: Default::default(),
        };
        let storage = oceanfs_core::StorageConfig {
            pools: vec![
                pool("pool-data", oceanfs_core::PoolRole::Data),
                pool("pool-wal", oceanfs_core::PoolRole::Wal),
                pool("pool-meta", oceanfs_core::PoolRole::Metadata),
                pool("pool-hints", oceanfs_core::PoolRole::Hints),
            ],
            health: Default::default(),
            missing_root_policy: Default::default(),
        };
        Arc::new(PoolRegistry::from_config(&storage, &tmp.join("data")).expect("registry"))
    }

    /// A listing failure leaves `storage_locations` untouched (no guesswork
    /// about what is missing).
    #[tokio::test]
    async fn listing_failure_changes_nothing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let registry = test_registry(tmp.path());
        let lifecycle =
            Arc::new(SegmentLifecycleCoordinator::new(&oceanfs_core::LifecycleConfig::default()));
        let store: Arc<dyn SegmentDataStore> = Arc::new(FailingListingStore);

        let (released, failures) =
            sweep_missing_segments(&registry, &lifecycle, &store, &NodeId::new("n1"), 0).await;
        assert_eq!((released, failures), (0, 0), "no listing means no changes");
    }
}
