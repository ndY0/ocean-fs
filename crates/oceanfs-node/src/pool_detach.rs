//! Node-side pool detach (d5, ADR-0036 D1/D6/D8): the emptiness proof and
//! the removal orchestration between the pool registry, the lifecycle
//! registry, the removed-pool store, and the manifest.
//!
//! `PoolRegistry::detach` (storage) is deliberately pure state: it
//! re-verifies `Detachable` + role + existence under its write lock and
//! removes the pool. What it cannot know is *emptiness* — whether any
//! registered segment still occupies the pool. That proof lives here,
//! because it requires the segment lifecycle registry and this node's id:
//!
//! - d4's emptiness definition (Deviations f.i): a pool is empty when
//!   **no `Reserved` entry** carries `pool_id == X` and **no `Sealed`
//!   entry** the node still holds (`self ∈ storage_locations`) carries
//!   `pool_id == X`. Released `Sealed` entries that keep `pool_id == X`
//!   after a cluster-drain release (self already dropped) hold no bytes on
//!   the pool and do not count — exactly what the d4 mover used to set
//!   `Detachable`.
//!
//! The check is a **node-side pre-check**, not a predicate injected into
//! the registry: running it under the registry's `pools` write lock would
//! hold that lock while scanning the lifecycle registry — a new cross-lock
//! order with no benefit, because a concurrent re-materializing push
//! writes through the lifecycle registry, not the pool registry. The
//! post-detach in-flight window is closed by two existing guards, not by a
//! pool_id remap (none exists — writes are registry-resolved): a stale
//! push whose copy landed while the pool was still registered makes
//! `pool_is_empty` return false, so detach is refused (`PoolNotEmpty`,
//! no-destructive-failure); and once the pool is actually removed, any
//! later write for an entry still naming the removed pool fails loudly at
//! `DiskSegmentStore::resolve_pool` (data_store.rs) before the holder set
//! is stamped.

use std::fmt;

use oceanfs_core::{NodeId, PoolRole};
use oceanfs_storage::{
    DrainState, PoolRegistry, SegmentLifecycleRegistry, SegmentState, StoragePool,
};

/// Why a node-side detach attempt was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PoolDetachError {
    /// No registered pool carries this id.
    UnknownPool(u32),
    /// The pool is not a `data` pool (wal/metadata/hints never detach).
    WrongRole(u32, PoolRole),
    /// The pool's drain record is not `Detachable` (drain it to empty first).
    NotDetachable(u32),
    /// The pool is `Detachable` but a segment still occupies it (a stale
    /// push re-materialized a copy after the worker's emptiness check).
    PoolNotEmpty(u32),
}

impl fmt::Display for PoolDetachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolDetachError::UnknownPool(id) => write!(f, "pool {id} is not registered"),
            PoolDetachError::WrongRole(id, _role) => write!(
                f,
                "pool {id} is not a data pool; only data pools can detach (wal/metadata/hints \
                 replacement is the g7/g8 path)"
            ),
            PoolDetachError::NotDetachable(id) => {
                write!(f, "pool {id} is not Detachable; drain it to empty first (ADR-0036 D6)")
            }
            PoolDetachError::PoolNotEmpty(id) => write!(
                f,
                "pool {id} is Detachable but still holds segments; a stale copy re-materialized \
                 after the drain completed — refusing to detach (no-destructive-failure)"
            ),
        }
    }
}

impl std::error::Error for PoolDetachError {}

/// A pool that was detached: its identity (for the removed-pool record).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DetachedPool {
    /// The detached pool's configured name.
    pub name: String,
    /// The detached pool's configured root directory.
    pub root: std::path::PathBuf,
}

/// The d4 emptiness definition (Deviations f.i): whether the node holds no
/// byte on `pool_id` — no `Reserved` in-flight entry and no `Sealed` entry
/// this node still lists itself in `storage_locations` for.
pub(crate) fn pool_is_empty(
    lifecycle: &SegmentLifecycleRegistry,
    self_id: &NodeId,
    pool_id: u32,
) -> bool {
    let mut resident = false;
    let mut reserved = false;
    lifecycle.for_each(|_id, entry| {
        if entry.metadata.pool_id != pool_id {
            return;
        }
        match entry.state {
            SegmentState::Reserved => {
                reserved = true;
            }
            SegmentState::Sealed
                if entry.metadata.storage_locations.iter().any(|loc| loc == self_id) =>
            {
                resident = true;
            }
            SegmentState::Sealed | SegmentState::Deleted => {}
            _ => {}
        }
    });
    !resident && !reserved
}

/// Detaches a pool after the node-side proof:
///
/// 1. exists + `data` role + drain record `Detachable` (the same ordering
///    `PoolRegistry::detach` enforces under its write lock), then
/// 2. the d4 emptiness proof (no resident/Reserved segment on the pool),
///    then
/// 3. [`PoolRegistry::detach`] (re-checks 1 under the write lock).
///
/// Returns the detached pool's name/root so the caller can record the
/// removal (ADR-0036 D8), re-gossip the manifest, and unregister metrics.
pub(crate) fn try_detach_pool(
    registry: &PoolRegistry,
    lifecycle: &SegmentLifecycleRegistry,
    self_id: &NodeId,
    pool_id: u32,
) -> Result<DetachedPool, PoolDetachError> {
    let pool = registry.pool_by_id(pool_id).ok_or(PoolDetachError::UnknownPool(pool_id))?;
    let detached = detach_identity(&pool)?;
    if registry.drain_state(pool_id) != DrainState::Detachable {
        return Err(PoolDetachError::NotDetachable(pool_id));
    }
    if !pool_is_empty(lifecycle, self_id, pool_id) {
        return Err(PoolDetachError::PoolNotEmpty(pool_id));
    }
    registry.detach(pool_id).map_err(|err| match err {
        oceanfs_storage::DetachError::UnknownPool(id) => PoolDetachError::UnknownPool(id),
        oceanfs_storage::DetachError::WrongRole(id, role) => PoolDetachError::WrongRole(id, role),
        oceanfs_storage::DetachError::NotDetachable(id) => PoolDetachError::NotDetachable(id),
    })?;
    Ok(detached)
}

fn detach_identity(pool: &StoragePool) -> Result<DetachedPool, PoolDetachError> {
    if pool.role() != PoolRole::Data {
        return Err(PoolDetachError::WrongRole(pool.id(), pool.role()));
    }
    Ok(DetachedPool { name: pool.name().to_string(), root: pool.root().to_path_buf() })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::Path;

    use oceanfs_core::{
        LifecycleConfig, MissingRootPolicy, PoolHealthConfig, PoolTech, SegmentId, SegmentMetadata,
        SizeTier, StorageConfig, StoragePoolConfig,
    };
    use oceanfs_storage::SegmentLifecycleRegistry;

    use super::*;

    /// Role-complete topology with two data pools (ids 0 and 1).
    fn registry_and_lifecycle(tmp: &Path) -> (PoolRegistry, SegmentLifecycleRegistry, NodeId) {
        let data_dir = tmp.join("data");
        let pool = |name: &str, role: PoolRole, root: &Path| StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight: Some(1),
            tech: PoolTech::Auto,
            health: PoolHealthConfig::default(),
        };
        let storage = StorageConfig {
            pools: vec![
                pool("data-0", PoolRole::Data, &tmp.join("nvme0")),
                pool("data-1", PoolRole::Data, &tmp.join("nvme1")),
                pool("journal", PoolRole::Wal, &tmp.join("optane0")),
                pool("meta", PoolRole::Metadata, &tmp.join("optane1")),
                pool("hints", PoolRole::Hints, &tmp.join("hints0")),
            ],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
        let lifecycle = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let self_id = NodeId::new("node-a");
        (registry, lifecycle, self_id)
    }

    /// A minimal sealed metadata on `pool_id`, optionally listing `self_id`
    /// in `storage_locations` (the node holds a copy on that pool).
    fn metadata(pool_id: u32, holder: Option<&NodeId>) -> SegmentMetadata {
        let mut locations: smallvec::SmallVec<[NodeId; 16]> = Default::default();
        if let Some(holder) = holder {
            locations.push(holder.clone());
        }
        SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: locations,
            sealed_at: None,
            pool_id,
            total_bytes: 0,
        }
    }

    /// Seals a `Sealed` entry on `pool_id` held by `self_id`.
    fn seal_held_on(lifecycle: &SegmentLifecycleRegistry, self_id: &NodeId, pool_id: u32) {
        let id = SegmentId::new();
        let meta = metadata(pool_id, Some(self_id));
        lifecycle.reserve(id, meta.clone()).expect("reserve");
        lifecycle.seal(id, meta).expect("seal");
    }

    #[test]
    fn pool_is_empty_when_no_lifecycle_entries_reference_it() {
        let tmp = tempfile::tempdir().unwrap();
        let (_registry, lifecycle, self_id) = registry_and_lifecycle(tmp.path());
        // No seeded entries anywhere: both data pools read empty.
        assert!(pool_is_empty(&lifecycle, &self_id, 0));
        assert!(pool_is_empty(&lifecycle, &self_id, 1));
    }

    #[test]
    fn pool_is_empty_false_when_a_reserved_entry_targets_the_pool() {
        let tmp = tempfile::tempdir().unwrap();
        let (_registry, lifecycle, self_id) = registry_and_lifecycle(tmp.path());
        let id = SegmentId::new();
        lifecycle.reserve(id, metadata(1, None)).expect("reserve on pool 1");
        assert!(pool_is_empty(&lifecycle, &self_id, 0));
        assert!(
            !pool_is_empty(&lifecycle, &self_id, 1),
            "a Reserved entry keeps the pool non-empty"
        );
    }

    #[test]
    fn pool_is_empty_ignores_released_entries_self_no_longer_holds() {
        let tmp = tempfile::tempdir().unwrap();
        let (_registry, lifecycle, self_id) = registry_and_lifecycle(tmp.path());
        // A Sealed entry that keeps pool_id==0 but no longer lists self in
        // storage_locations (d4 cluster-release residue) holds no bytes on
        // the pool → empty by the d4 definition.
        let id = SegmentId::new();
        let meta = metadata(0, None);
        lifecycle.reserve(id, meta.clone()).expect("reserve");
        lifecycle.seal(id, meta).expect("seal");
        assert!(pool_is_empty(&lifecycle, &self_id, 0));
    }

    #[test]
    fn try_detach_rejects_unknown_and_non_data_pools() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, lifecycle, self_id) = registry_and_lifecycle(tmp.path());
        assert_eq!(
            try_detach_pool(&registry, &lifecycle, &self_id, 99),
            Err(PoolDetachError::UnknownPool(99))
        );
        // The wal pool (id 2) never detaches.
        assert_eq!(
            try_detach_pool(&registry, &lifecycle, &self_id, 2),
            Err(PoolDetachError::WrongRole(2, PoolRole::Wal))
        );
    }

    #[test]
    fn try_detach_rejects_healthy_and_draining_pools() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, lifecycle, self_id) = registry_and_lifecycle(tmp.path());
        // Healthy (Idle): refused.
        assert_eq!(
            try_detach_pool(&registry, &lifecycle, &self_id, 0),
            Err(PoolDetachError::NotDetachable(0))
        );
        // Draining (worker not finished): refused.
        registry.begin_drain(0).unwrap();
        assert_eq!(
            try_detach_pool(&registry, &lifecycle, &self_id, 0),
            Err(PoolDetachError::NotDetachable(0))
        );
    }

    #[test]
    fn try_detach_succeeds_on_empty_detachable_pool() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, lifecycle, self_id) = registry_and_lifecycle(tmp.path());
        registry.begin_drain(0).unwrap();
        registry.set_pool_empty(0).unwrap();

        let detached = try_detach_pool(&registry, &lifecycle, &self_id, 0).expect("detach");
        assert_eq!(detached.name, "data-0");
        assert!(registry.pool_by_id(0).is_none());
        assert_eq!(registry.data_pools().len(), 1);
    }

    #[test]
    fn pool_is_empty_counts_a_stale_resident_copy_as_non_empty() {
        // A Detachable pool with a stale Sealed entry whose storage_locations
        // still include self must NOT read empty (the d4 f.ii guard).
        let tmp = tempfile::tempdir().unwrap();
        let (registry, lifecycle, self_id) = registry_and_lifecycle(tmp.path());
        registry.begin_drain(0).unwrap();
        registry.set_pool_empty(0).unwrap();

        // Simulate the stale re-add: a Sealed entry on pool 0 that lists
        // self in storage_locations.
        seal_held_on(&lifecycle, &self_id, 0);

        assert!(
            !pool_is_empty(&lifecycle, &self_id, 0),
            "a resident copy keeps the pool non-empty"
        );
        assert_eq!(
            try_detach_pool(&registry, &lifecycle, &self_id, 0),
            Err(PoolDetachError::PoolNotEmpty(0)),
            "detach must refuse over a re-materialized copy"
        );
    }
}
