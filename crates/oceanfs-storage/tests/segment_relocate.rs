//! Integration test: durable segment relocation (d2, ADR-0036 D2/D3).
//!
//! A 2-data-pool store + lifecycle coordinator + real event-WAL relocates
//! a sealed segment back and forth between pool roots while concurrent
//! reader tasks continuously read it through the store's registry-driven
//! read path. Every read across the source→target commit switches must
//! return byte-identical data (continuous correctness), and the settled
//! state must be a single `.dat` on the final pool root with the registry
//! `pool_id` matching.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{path::PathBuf, sync::Arc};

use oceanfs_core::{LifecycleConfig, PoolRole, SegmentId, SizeTier, StorageConfig};
use oceanfs_storage::{
    io::{InMemorySegmentReader, IoBackend, IoObserver, IoReadMode},
    DiskSegmentStore, EventWal, PoolRegistry, SegmentLifecycleCoordinator,
    SegmentLifecycleRegistry, SegmentRelocator,
};
use oceanfs_storage_api::SegmentDataStore;

fn pool_config(
    name: &str,
    role: PoolRole,
    root: &std::path::Path,
) -> oceanfs_core::StoragePoolConfig {
    oceanfs_core::StoragePoolConfig {
        name: name.to_string(),
        role,
        root: root.to_path_buf(),
        weight: Some(1),
        tech: oceanfs_core::PoolTech::Auto,
        health: Default::default(),
    }
}

/// A 2-data-pool relocation store: real event-WAL + coordinator +
/// unified store over sibling pool roots.
#[allow(clippy::type_complexity)]
async fn make_store() -> (
    tempfile::TempDir,
    Arc<DiskSegmentStore>,
    Arc<SegmentLifecycleCoordinator>,
    Arc<SegmentLifecycleRegistry>,
    Vec<PathBuf>,
) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let roots = vec![tmp.path().join("pool-data-0"), tmp.path().join("pool-data-1")];
    let storage = StorageConfig {
        pools: vec![
            pool_config("data-0", PoolRole::Data, &roots[0]),
            pool_config("data-1", PoolRole::Data, &roots[1]),
            pool_config("wal-0", PoolRole::Wal, &tmp.path().join("pool-wal")),
            pool_config("meta-0", PoolRole::Metadata, &tmp.path().join("pool-meta")),
            pool_config("hints-0", PoolRole::Hints, &tmp.path().join("pool-hints")),
        ],
        health: Default::default(),
        missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    };
    let pool_registry =
        Arc::new(PoolRegistry::from_config(&storage, &tmp.path().join("meta")).expect("registry"));
    let lifecycle_registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
    let wal_dir = tmp.path().join("event-wal");
    let event_wal_config = oceanfs_core::EventWalConfig {
        event_wal_dir: wal_dir.clone(),
        event_wal_file_size_bytes: 1024 * 1024,
        event_wal_fsync_batch_timeout_ms: 10,
        event_wal_checkpoint_bytes: 1024 * 1024,
    };
    let event_wal =
        Arc::new(EventWal::open(wal_dir.clone(), &event_wal_config).await.expect("wal"));
    let lifecycle = Arc::new(
        SegmentLifecycleCoordinator::with_registry(Arc::clone(&lifecycle_registry))
            .with_event_wal(event_wal),
    );
    let observer = Arc::new(IoObserver::new());
    observer.register_pool(0, None);
    observer.register_pool(1, None);
    let reader: Arc<dyn oceanfs_storage::io::SegmentReader> =
        Arc::new(InMemorySegmentReader::new());
    let store = Arc::new(DiskSegmentStore::new(
        Arc::clone(&pool_registry),
        Arc::clone(&lifecycle_registry),
        reader,
        IoReadMode::Buffered,
        Arc::new(IoBackend::default()),
        observer,
    ));
    (tmp, store, lifecycle, lifecycle_registry, roots)
}

#[tokio::test]
async fn relocate_stays_correct_under_concurrent_reads() {
    let (_tmp, store, lifecycle, lifecycle_registry, roots) = make_store().await;
    let data: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 251) as u8).collect();

    // Seed a sealed segment whose `.dat` lives on pool 0.
    let id = SegmentId::new();
    lifecycle.request_reserve(id, SizeTier::Standard, 4, 2).await.expect("reserve");
    let merkle_root = oceanfs_core::HashOutput::from_bytes(*blake3::hash(&data).as_bytes());
    let meta = oceanfs_core::SegmentMetadata {
        pool_id: 0,
        total_bytes: data.len() as u64,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(merkle_root),
        storage_locations: smallvec::smallvec![],
        sealed_at: Some(1_700_000_000_000),
    };
    lifecycle.request_seal(id, meta, None).await.expect("seal");
    store.write_segment_data(&id, &data).await.expect("seed write");
    assert!(roots[0].join(format!("{id}.dat")).exists());

    let relocator = Arc::new(SegmentRelocator::new(Arc::clone(&lifecycle), Arc::clone(&store)));

    // Continuous readers: every read across the source→target switches
    // must return byte-identical data (the commit is atomic for
    // registry-driven reads).
    let mut readers = Vec::new();
    for _ in 0..4 {
        let store = Arc::clone(&store);
        let expected = data.clone();
        readers.push(tokio::spawn(async move {
            for _ in 0..60 {
                let file = store
                    .read_segment_data(&id)
                    .await
                    .expect("read ok")
                    .expect("segment always readable during relocation");
                assert_eq!(&file.data[..], &expected[..], "continuous read correctness");
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        }));
    }

    // Relocate back and forth while the readers are running. Each move
    // targets the *other* pool from the segment's current pool, so
    // concurrent moves never hit SamePool — the per-segment lock
    // serializes them into a valid sequence.
    let mut moves = Vec::new();
    for _ in 0..5 {
        let relocator = Arc::clone(&relocator);
        let registry = Arc::clone(&lifecycle_registry);
        moves.push(tokio::spawn(async move {
            let current = registry.get(id).expect("entry").metadata.pool_id;
            let target = 1 - current;
            relocator.relocate(id, target).await.expect("relocate must succeed");
        }));
    }
    for m in moves {
        m.await.expect("move completed");
    }
    for r in readers {
        r.await.expect("reader completed");
    }

    // Settled state: exactly one `.dat`, on the final target pool root,
    // and the registry's pool_id matches.
    let entry = lifecycle_registry.get(id).expect("entry");
    let final_pool = entry.metadata.pool_id;
    assert!(matches!(final_pool, 0 | 1));
    assert!(roots[final_pool as usize].join(format!("{id}.dat")).exists());
    assert!(!roots[1 - final_pool as usize].join(format!("{id}.dat")).exists());
    let file = store.read_segment_data(&id).await.unwrap().expect("readable");
    assert_eq!(&file.data[..], &data[..], "byte-identical content after all relocations");
}
