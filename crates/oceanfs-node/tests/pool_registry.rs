//! Integration test: `PoolRegistry` built from a 4-pool tempdir topology.
//!
//! Epic `disk-resilience`, f2 (`pool-runtime`) DoD integration item: build a
//! `PoolRegistry` from a full role-complete config and assert all roots are probed
//! (created, probed, probe files cleaned up) and registered with the
//! expected ids/roles — exercising the public API end to end.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::{MissingRootPolicy, PoolRole, PoolTech, StorageConfig, StoragePoolConfig};
use oceanfs_storage::{PoolRegistry, PoolStatus};

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

#[test]
fn pool_registry_builds_from_four_pool_config_and_probes_all_roots() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // data_dir is a sibling of the pool roots: pool mode and legacy mode
    // must be disjoint layouts (f1 validation rule).
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

    // All five roots were probed: created, and no probe litter remains.
    for root in &roots {
        assert!(root.exists(), "root {root:?} must be created by the probe");
        let leftovers: Vec<_> = std::fs::read_dir(root)
            .expect("read root")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".probe-"))
            .collect();
        assert!(leftovers.is_empty(), "probe files must be cleaned up in {root:?}");
    }

    // All five pools registered with config-order ids and correct roles.
    let pools = registry.pools();
    assert_eq!(pools.len(), 5);
    let ids: Vec<u32> = pools.iter().map(|pool| pool.id()).collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
    assert_eq!(pools[0].name(), "fast-nvme-0");
    assert_eq!(pools[0].role(), PoolRole::Data);
    assert_eq!(pools[2].name(), "journal");
    assert_eq!(pools[2].role(), PoolRole::Wal);
    assert_eq!(pools[3].role(), PoolRole::Metadata);
    assert_eq!(pools[3].status(), PoolStatus::Healthy);
    assert_eq!(pools[4].role(), PoolRole::Hints);
    assert_eq!(pools[4].status(), PoolStatus::Healthy);

    // Lookups agree with the construction.
    assert_eq!(registry.pool_by_id(2).expect("wal pool").name(), "journal");
    assert_eq!(registry.pool_by_role(PoolRole::Wal).expect("wal").id(), 2);
    assert_eq!(registry.pool_by_role(PoolRole::Hints).expect("hints").id(), 4);
    let data_pools = registry.data_pools();
    assert_eq!(data_pools.len(), 2);
    assert_eq!(data_pools[0].id(), 0);
    assert_eq!(data_pools[1].id(), 1);

    // Capacity is populated from statvfs and refreshes without error.
    registry.refresh_capacity();
    assert!(registry.pool_by_id(0).expect("pool 0").total_bytes() > 0);
}
