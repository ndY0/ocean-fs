//! Integration test (g1 `disk-io-observability` DoD): a small write
//! cycle through a `FaultyIo`-wrapped store counts the injected errors
//! **per pool** on the shared [`IoObserver`].
//!
//! The injector lives at the storage/io layer (the unit-level Level-1
//! fault injector from the feature spec): a `FaultyIo<ObservedIo>`
//! performs exactly the ops the seal pipeline performs (temp-file
//! writes plus the flush barrier's fsync) through the observed
//! [`DiskIo`] surface. Pool A has injected failures; pool B stays
//! healthy — the observer attributes the errors to pool A only.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{io, path::Path, sync::Arc};

use oceanfs_core::{MissingRootPolicy, PoolRole, PoolTech, StorageConfig, StoragePoolConfig};
use oceanfs_storage::{
    io::{DiskIo, FaultyIo, IoBackend, IoObserver, ObservedIo},
    PoolRegistry,
};

fn pool(name: &str, role: PoolRole, root: &Path) -> StoragePoolConfig {
    StoragePoolConfig {
        name: name.to_string(),
        role,
        root: root.to_path_buf(),
        weight: None,
        tech: PoolTech::Auto,
        health: Default::default(),
    }
}

fn pool_io(pool_id: u32, observer: &Arc<IoObserver>) -> ObservedIo {
    ObservedIo { pool_id, backend: Arc::new(IoBackend::default()), observer: observer.clone() }
}

#[tokio::test]
async fn faulty_io_write_cycle_counts_errors_per_pool() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let root_a = tmp.path().join("pool-a");
    let root_b = tmp.path().join("pool-b");
    let storage = StorageConfig {
        pools: vec![
            pool("pool-a", PoolRole::Data, &root_a),
            pool("pool-b", PoolRole::Data, &root_b),
            pool("journal", PoolRole::Wal, &tmp.path().join("optane0")),
            pool("meta", PoolRole::Metadata, &tmp.path().join("optane1")),
            pool("hints", PoolRole::Hints, &tmp.path().join("hints0")),
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    let registry = PoolRegistry::from_config(&storage, &data_dir).expect("2-data-pool registry");
    let observer = Arc::new(IoObserver::new());
    registry.observe_into(&observer);

    // ---- Pool A: a FaultyIo-wrapped write cycle with 2 injected
    // failures (the write_handle path — the same op the sealer calls).
    let faulty = FaultyIo::new(pool_io(0, &observer));
    faulty.fail_next(2, io::ErrorKind::Other);
    let file_a = std::fs::File::create(root_a.join("seg-a.dat")).expect("create a");
    assert!(faulty.write_handle(&file_a, b"header").is_err(), "first write fails");
    assert!(faulty.write_handle(&file_a, b"data").is_err(), "second write fails");
    faulty.write_handle(&file_a, b"index").expect("third write passes");
    faulty.fsync_handle(&file_a).expect("fsync passes");
    drop(file_a);

    // ---- Pool B: a healthy write cycle (no faults).
    let file_b = std::fs::File::create(root_b.join("seg-b.dat")).expect("create b");
    pool_io(1, &observer).write_handle(&file_b, b"data").expect("b write");
    pool_io(1, &observer).fsync_handle(&file_b).expect("b fsync");
    drop(file_b);

    // The observer attributes errors to pool A only.
    assert_eq!(observer.io_error_count(0), 2, "pool A: 2 injected errors");
    assert_eq!(observer.io_error_count(1), 0, "pool B: healthy");

    let signal_a = observer.snapshot(0).expect("pool A registered");
    assert_eq!(signal_a.errors, 2);
    assert_eq!(signal_a.ops, 4, "4 observed ops on pool A");
    assert!(signal_a.error_rate > 0.0);

    let signal_b = observer.snapshot(1).expect("pool B registered");
    assert_eq!(signal_b.errors, 0);
    assert_eq!(signal_b.ops, 2, "2 observed ops on pool B");
    assert_eq!(signal_b.error_rate, 0.0);
}
