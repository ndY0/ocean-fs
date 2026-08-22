//! Integration test: placement policy drives segment distribution across
//! data pools (epic `disk-resilience`, f3 DoD).
//!
//! Builds a 2-data-pool registry (f2), then seals several small segments
//! through the existing `SegmentSealer` with the policy injected as the
//! per-segment target root — asserting the distribution lands on both
//! pools. f5 completes the multi-root store; this exercises f2+f3 together.
//!
//! Capacity accounting: on a single test filesystem `statvfs` cannot see
//! per-pool deltas (both roots share one filesystem), so the test drives
//! the capacity evolution exactly the way the node's maintenance task
//! would after seals — via `PoolRegistry::set_pool_capacity` (1 MiB per
//! sealed segment).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{path::PathBuf, sync::Arc};

use oceanfs_core::{
    EventWalConfig, HashOutput, LifecycleConfig, MissingRootPolicy, PoolRole, PoolTech,
    SegmentIndexEntry, SegmentSizeConfig, SizeTier, StorageConfig, StoragePoolConfig, WalConfig,
};
use oceanfs_storage::{
    io::{IoReadMode, SegmentWriteMode},
    ActiveSegment, BufferPool, PlacementPolicy, PoolRegistry, SealConfig,
    SegmentLifecycleCoordinator, SegmentSealer, WalWriter,
};

/// Capacity accounting per sealed segment (simulated; see module docs).
const SEGMENT_ACCOUNTING_BYTES: u64 = 1024 * 1024;
/// Initial free space on each simulated pool (10 GiB).
const INITIAL_FREE: u64 = 10 * 1024 * 1024 * 1024;
/// Initial total capacity of each simulated pool (100 GiB).
const INITIAL_TOTAL: u64 = 100 * 1024 * 1024 * 1024;

/// Builds a 2-data-pool registry under a tempdir, with simulated capacity.
fn two_data_pool_registry(tmp: &tempfile::TempDir) -> (PoolRegistry, PathBuf, PathBuf) {
    let data_dir = tmp.path().join("data");
    let root_a = tmp.path().join("nvme0");
    let root_b = tmp.path().join("nvme1");
    let storage = StorageConfig {
        pools: vec![
            StoragePoolConfig {
                name: "pool-a".into(),
                role: PoolRole::Data,
                root: root_a.clone(),
                weight: Some(1),
                tech: PoolTech::Auto,
                health: Default::default(),
            },
            StoragePoolConfig {
                name: "pool-b".into(),
                role: PoolRole::Data,
                root: root_b.clone(),
                weight: Some(1),
                tech: PoolTech::Auto,
                health: Default::default(),
            },
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    for id in 0..2 {
        registry.set_pool_capacity(id, INITIAL_TOTAL, INITIAL_FREE);
    }
    (registry, root_a, root_b)
}

/// Seals one small segment into `root` through the real sealer and returns
/// the sealed segment id. The lifecycle coordinator and WAL are shared
/// across seals (opened once); each seal gets a fresh sealer whose
/// `SealConfig.data_dir` is the policy-selected pool root.
async fn seal_one_segment(
    root: &std::path::Path,
    lifecycle: &Arc<SegmentLifecycleCoordinator>,
    wal: &Arc<WalWriter>,
) -> oceanfs_core::SegmentId {
    let config = SealConfig {
        target_size_bytes: 100,
        seal_timeout_ms: 1000,
        data_dir: root.to_path_buf(),
        io_mode: IoReadMode::Buffered,
        write_mode: SegmentWriteMode::Rename,
        ..Default::default()
    };
    let sealer = SegmentSealer::new(config, Arc::clone(wal), Arc::clone(lifecycle));

    let pool = BufferPool::new(65536, 4);
    let size_config =
        SegmentSizeConfig { default_target_size: 100, ..SegmentSizeConfig::default() };
    let mut active = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).expect("segment");

    // Write enough to exceed the 100-byte target, then reserve + seal.
    active.append(&[0u8; 120]).expect("append");
    lifecycle.request_reserve(active.id(), SizeTier::Standard, 0, 0).await.expect("reserve");

    let entries = vec![SegmentIndexEntry { offset: 0, length: 120, blob_key_hash: [0xAB; 32] }];
    let handle = sealer
        .try_seal(&mut active, 0, &entries, Some(HashOutput::from_bytes([0xAB; 32])))
        .await
        .expect("seal")
        .expect("segment must seal when full");
    handle.id()
}

/// The policy injected into the seal flow distributes segments across both
/// data pools, and every sealed segment lands in its selected pool's root.
#[tokio::test]
async fn placement_policy_distributes_segments_across_data_pools() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (registry, _root_a, _root_b) = two_data_pool_registry(&tmp);
    let policy = PlacementPolicy::new();

    let lifecycle = Arc::new(
        SegmentLifecycleCoordinator::new(&LifecycleConfig::default()).with_event_wal(Arc::new(
            oceanfs_storage::EventWal::open(
                tmp.path().join("event-wal"),
                &EventWalConfig {
                    event_wal_dir: tmp.path().join("event-wal"),
                    event_wal_file_size_bytes: 1024 * 1024,
                    event_wal_fsync_batch_timeout_ms: 10,
                    event_wal_checkpoint_bytes: 1024 * 1024,
                },
            )
            .await
            .expect("open event wal"),
        )),
    );
    let wal = Arc::new(
        WalWriter::open(&WalConfig {
            data_dir: tmp.path().join("wal"),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        })
        .await
        .expect("open wal"),
    );

    // Seal 8 segments; each seal's target root comes from the policy, and
    // the winner's capacity is consumed by the segment (the node's
    // maintenance task would observe the same through refresh_capacity).
    let mut sealed = Vec::new();
    for _ in 0..8 {
        let pool = policy.select_data_pool(&registry).expect("a data pool must be eligible");
        let id = seal_one_segment(pool.root(), &lifecycle, &wal).await;
        sealed.push((pool.id(), pool.root().to_path_buf(), id));

        let free = pool.free_bytes() - SEGMENT_ACCOUNTING_BYTES;
        registry.set_pool_capacity(pool.id(), pool.total_bytes(), free);
    }

    // Every sealed segment landed in its selected pool's root.
    for (pool_id, root, id) in &sealed {
        assert!(
            root.join(format!("{id}.dat")).exists(),
            "segment {id} must exist in pool {pool_id} root {root:?}"
        );
    }

    // The distribution hit both pools (f2+f3 together): with equal free
    // space the tie goes to pool-a, then each seal hands the lead to the
    // other pool — 8 seals must alternate 4/4.
    let count_a = sealed.iter().filter(|(id, _, _)| *id == 0).count();
    let count_b = sealed.iter().filter(|(id, _, _)| *id == 1).count();
    assert_eq!(count_a, 4, "pool-a must receive half the segments, got {count_a}");
    assert_eq!(count_b, 4, "pool-b must receive half the segments, got {count_b}");

    // Determinism: identical registry state → identical selection.
    let first = policy.select_data_pool(&registry).expect("pool").id();
    let second = policy.select_data_pool(&registry).expect("pool").id();
    assert_eq!(first, second, "selection must be deterministic");
}
