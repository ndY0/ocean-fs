//! Builds the node's storage-pool manifest (ADR-0029 D2) from the
//! `PoolRegistry`.
//!
//! The composition root maps each registered pool to a [`PoolManifest`]
//! (id, role/status constants, write-degraded flag, free capacity,
//! weight) and wraps them in a [`NodeManifest`] with the announcement
//! incarnation. The manifest is built ONCE per pool change (perf rule
//! 2.4) and handed to `Membership::set_self_manifest`, which gossips it
//! as an opaque attribute; Phase A registers at boot only — f8
//! (runtime-attach) re-invokes this on pool set changes.

use std::sync::Arc;

use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
use oceanfs_storage::{PoolRegistry, PoolStatus, StoragePool};

/// Builds the node's storage-pool manifest from the registry.
///
/// One [`PoolManifest`] per registered pool, in registry (config)
/// order, with the f2 enum values encoded as the wire constants
/// (`PoolRole::as_str`, the `Healthy`/`Degraded`/`Dead`/`Draining` status
/// strings). `incarnation` is the announcement incarnation the node joined
/// with — the value that also rides the membership entry, so peers can tie
/// the manifest to the restart it was declared with (ADR-0022 D1).
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::NodeManifest;
/// use oceanfs_node::pool_manifest::build_node_manifest;
/// use oceanfs_storage::PoolRegistry;
///
/// # let tmp = tempfile::tempdir().expect("tempdir");
/// # let data_dir = tmp.path().join("data");
/// # let storage = oceanfs_core::StorageConfig {
/// #     pools: vec![
/// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
/// #     ],
/// #     missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
/// # };
/// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
///
/// let manifest = build_node_manifest(3, &registry);
/// assert_eq!(manifest.incarnation(), 3);
/// assert_eq!(manifest.pools().len(), 4);
/// assert_eq!(manifest.pools()[0].role(), "data");
/// assert_eq!(manifest.pools()[0].status(), "healthy");
/// ```
pub fn build_node_manifest(incarnation: u64, registry: &PoolRegistry) -> NodeManifest {
    let pools = registry.pools();
    // Perf rule 1.3: the manifest vector is pre-sized — the pool count
    // is known up front (5–20 entries at scale).
    let mut pool_manifests = Vec::with_capacity(pools.len());
    for pool in &pools {
        pool_manifests.push(pool_manifest_from_pool(pool));
    }
    // g8 (metadata-loss-recovery): the node-level flag mirrors the
    // registry's service gate — unavailable exactly while the metadata
    // pool is Dead (a restart with a replaced root keeps the pool Dead
    // until the rebuilt store rejoins). Peers read this as a hard
    // routing exclusion independent of the per-pool rows above.
    let unavailable = !registry.node_serves_requests();
    NodeManifest::from_pools(incarnation, &pool_manifests).with_node_unavailable(unavailable)
}

/// Maps one registered pool to its manifest wire form.
///
/// The role/status constants use the f2 enum string forms so the wire
/// format never forces a redesign (ADR-0029 D2: strings for forward
/// compatibility).
fn pool_manifest_from_pool(pool: &Arc<StoragePool>) -> PoolManifest {
    let status = match pool.status() {
        PoolStatus::Healthy => "healthy",
        PoolStatus::Degraded => "degraded",
        PoolStatus::Dead => "dead",
        // d1 (ADR-0036 D6): a draining pool is not a placement target —
        // peers see `"draining"` and their existing manifest-aware seams
        // (repair selector, routing cache, peer selection) already treat
        // any non-`"healthy"` data pool as excluded.
        PoolStatus::Draining => "draining",
        // Non-exhaustive (ADR-0029 §D3 reserves transitions): treat
        // unknown statuses as Healthy.
        _ => "healthy",
    };
    PoolManifest::new(
        pool.id(),
        pool.role().as_str(),
        status,
        pool.write_degraded(),
        pool.free_bytes(),
        pool.weight(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::{MissingRootPolicy, PoolRole, PoolTech, StorageConfig, StoragePoolConfig};

    use super::*;

    fn pool(name: &str, role: PoolRole, root: &std::path::Path) -> StoragePoolConfig {
        StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight: None,
            tech: PoolTech::Auto,
            health: Default::default(),
        }
    }

    /// The f6 DoD unit: a manifest built from a 4-pool registry has 4
    /// PoolManifests with correct role/status/weight/free.
    #[test]
    fn manifest_from_four_pool_registry_carries_each_pool() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_dir = tmp.path().join("data");
        let roots = [
            tmp.path().join("nvme0"),
            tmp.path().join("nvme1"),
            tmp.path().join("optane0"),
            tmp.path().join("optane1"),
            tmp.path().join("hints0"),
        ];
        let storage = StorageConfig {
            pools: vec![
                pool("fast-nvme-0", PoolRole::Data, &roots[0]),
                pool("fast-nvme-1", PoolRole::Data, &roots[1]),
                pool("journal", PoolRole::Wal, &roots[2]),
                pool("meta", PoolRole::Metadata, &roots[3]),
                pool("hints", PoolRole::Hints, &roots[4]),
            ],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");

        // Deterministic free capacities (f2's capacity override) so the
        // manifest mirrors what f7's routing cache will read.
        registry.set_pool_capacity(0, 2 << 30, 500 << 20);
        registry.set_pool_capacity(1, 2 << 30, 700 << 20);
        registry.set_pool_capacity(2, 1 << 30, 200 << 20);
        registry.set_pool_capacity(3, 1 << 30, 100 << 20);
        registry.set_pool_capacity(4, 1 << 30, 100 << 20);

        let manifest = build_node_manifest(11, &registry);

        assert_eq!(manifest.incarnation(), 11);
        let pools = manifest.pools();
        assert_eq!(pools.len(), 5, "one PoolManifest per registered pool");

        // Pool 0: data, healthy, weight from auto-detect, free set above.
        assert_eq!(pools[0].id(), 0);
        assert_eq!(pools[0].role(), "data");
        assert_eq!(pools[0].status(), "healthy");
        assert!(!pools[0].write_degraded());
        assert_eq!(pools[0].capacity_free_bytes(), 500 << 20);
        assert!(pools[0].weight() >= 1);

        // Pool 1: second data pool with its own free capacity.
        assert_eq!(pools[1].id(), 1);
        assert_eq!(pools[1].role(), "data");
        assert_eq!(pools[1].capacity_free_bytes(), 700 << 20);

        // Pool 2: the pinned WAL role.
        assert_eq!(pools[2].id(), 2);
        assert_eq!(pools[2].role(), "wal");
        assert_eq!(pools[2].status(), "healthy");
        assert_eq!(pools[2].capacity_free_bytes(), 200 << 20);

        // Pool 3: the metadata role.
        assert_eq!(pools[3].id(), 3);
        assert_eq!(pools[3].role(), "metadata");
        assert_eq!(pools[3].capacity_free_bytes(), 100 << 20);

        // Pool 4: the hints role.
        assert_eq!(pools[4].id(), 4);
        assert_eq!(pools[4].role(), "hints");
        assert_eq!(pools[4].status(), "healthy");

        // A Healthy registry yields an available manifest.
        assert!(!manifest.node_unavailable(), "healthy node is available");
    }

    /// d1 (ADR-0036 D6): a `Draining` data pool maps to the `"draining"`
    /// status string, so peers see it as not-a-placement-target through
    /// the existing manifest-aware seams.
    #[test]
    fn draining_pool_maps_to_draining_status_string() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_dir = tmp.path().join("data");
        let roots = [
            tmp.path().join("nvme0"),
            tmp.path().join("nvme1"),
            tmp.path().join("optane0"),
            tmp.path().join("optane1"),
            tmp.path().join("hints0"),
        ];
        let storage = StorageConfig {
            pools: vec![
                pool("data-0", PoolRole::Data, &roots[0]),
                pool("data-1", PoolRole::Data, &roots[1]),
                pool("journal", PoolRole::Wal, &roots[2]),
                pool("meta", PoolRole::Metadata, &roots[3]),
                pool("hints", PoolRole::Hints, &roots[4]),
            ],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");

        // Healthy before the drain.
        let manifest = build_node_manifest(1, &registry);
        assert_eq!(manifest.pools()[0].status(), "healthy");

        // After begin_drain the row reads "draining"; the sibling and the
        // pinned roles are unaffected.
        registry.begin_drain(0).expect("begin drain");
        let manifest = build_node_manifest(2, &registry);
        assert_eq!(manifest.pools()[0].status(), "draining");
        assert_eq!(manifest.pools()[1].status(), "healthy");
        assert_eq!(manifest.pools()[2].status(), "healthy", "the wal pool is untouched");

        // The node itself stays available (draining is not Dead).
        assert!(!manifest.node_unavailable(), "a draining data pool does not take the node down");
    }

    /// g8 (metadata-loss-recovery): the node-level `node_unavailable`
    /// flag derives from the registry's service gate — a Dead metadata
    /// pool (even with healthy data pools) marks the node unavailable;
    /// recovering it to Healthy clears the flag.
    #[test]
    fn metadata_pool_dead_sets_node_unavailable_in_manifest() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_dir = tmp.path().join("data");
        let roots = [
            tmp.path().join("nvme0"),
            tmp.path().join("journal"),
            tmp.path().join("meta"),
            tmp.path().join("hints0"),
        ];
        let storage = StorageConfig {
            pools: vec![
                pool("data-0", PoolRole::Data, &roots[0]),
                pool("journal", PoolRole::Wal, &roots[1]),
                pool("meta", PoolRole::Metadata, &roots[2]),
                pool("hints", PoolRole::Hints, &roots[3]),
            ],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
        let meta_id = registry.pool_by_role(PoolRole::Metadata).expect("metadata pool").id();

        // Drive the metadata pool Dead (the D3 consequence) — the data
        // pool stays Healthy.
        registry.set_status(meta_id, oceanfs_storage::PoolStatus::Dead);
        let manifest = build_node_manifest(2, &registry);
        assert!(manifest.node_unavailable(), "Dead metadata pool => unavailable");

        // Clear the flag once the pool recovers.
        registry.set_status(meta_id, oceanfs_storage::PoolStatus::Healthy);
        let manifest = build_node_manifest(3, &registry);
        assert!(!manifest.node_unavailable(), "recovered metadata pool => available");
    }
}
