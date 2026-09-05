//! Integration test: the unified `SegmentDataStore` round-trip.
//!
//! Store-unification f2 (ADR-0032 D2/D3): every durability consumer
//! speaks the unified `oceanfs_storage_api::SegmentDataStore` trait and
//! the ONLY production impl is `oceanfs_storage::DiskSegmentStore`.
//! This test drives one complete repair-style round-trip — reserve →
//! write → read → overwrite (the heal/re-rep shape: fetch a corrected
//! payload, persist it, verify it) — through a trait-object typed
//! store over a real pools-only layout, exercising the NotFound
//! contract (`Ok(None)`), the delete path, and the lifecycle-routed
//! write invariant (unregistered writes are rejected — the pool-0
//! write-before-register bridge is gone).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{LifecycleConfig, SegmentId, StorageConfig, StoragePoolConfig};
use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;
use oceanfs_storage_api::SegmentDataStore;

/// A pools-only store over one data pool (config-order id 0) plus the
/// mandatory wal/metadata/hints siblings — the node's boot shape since
/// ADR-0031 (pools mandatory).
async fn pools_store(
    tmp: &tempfile::TempDir,
) -> (
    Arc<dyn SegmentDataStore>,
    std::path::PathBuf,
    Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
) {
    let data_root = tmp.path().join("nvme0");
    let storage = StorageConfig {
        pools: vec![
            StoragePoolConfig {
                name: "pool-a".into(),
                role: oceanfs_core::PoolRole::Data,
                root: data_root.clone(),
                weight: Some(1),
                tech: oceanfs_core::PoolTech::Auto,
                health: Default::default(),
            },
            StoragePoolConfig {
                name: "journal".into(),
                role: oceanfs_core::PoolRole::Wal,
                root: tmp.path().join("optane0"),
                weight: Some(1),
                tech: oceanfs_core::PoolTech::Auto,
                health: Default::default(),
            },
            StoragePoolConfig {
                name: "meta".into(),
                role: oceanfs_core::PoolRole::Metadata,
                root: tmp.path().join("optane1"),
                weight: Some(1),
                tech: oceanfs_core::PoolTech::Auto,
                health: Default::default(),
            },
            StoragePoolConfig {
                name: "hints".into(),
                role: oceanfs_core::PoolRole::Hints,
                root: tmp.path().join("hints0"),
                weight: Some(1),
                tech: oceanfs_core::PoolTech::Auto,
                health: Default::default(),
            },
        ],
        missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    };
    let pool_registry = Arc::new(
        oceanfs_storage::PoolRegistry::from_config(&storage, &tmp.path().join("data"))
            .expect("registry"),
    );
    let pools = pool_registry.data_pools();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0].id(), 0, "config-order id");
    let lifecycle_registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
    let observer = Arc::new(oceanfs_storage::io::IoObserver::new());
    observer.register_pool(0, None);
    let store: Arc<dyn SegmentDataStore> = Arc::new(oceanfs_storage::DiskSegmentStore::new(
        pool_registry,
        Arc::clone(&lifecycle_registry),
        Arc::new(oceanfs_storage::io::InMemorySegmentReader::new()),
        oceanfs_storage::io::IoReadMode::Buffered,
        Arc::new(oceanfs_storage::io::IoBackend::default()),
        observer,
    ));
    // The coordinator (event-WAL-armed) seeds registered segments — the
    // reserve-before-write invariant (ADR-0032 D3).
    let event_wal = Arc::new(
        oceanfs_storage::segment::event_wal::EventWal::open(
            tmp.path().join("event-wal"),
            &oceanfs_core::EventWalConfig {
                event_wal_dir: tmp.path().join("event-wal"),
                event_wal_file_size_bytes: 1024 * 1024,
                event_wal_fsync_batch_timeout_ms: 10,
                event_wal_checkpoint_bytes: 1024 * 1024,
            },
        )
        .await
        .unwrap(),
    );
    let lifecycle = Arc::new(
        oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::with_registry(
            lifecycle_registry,
        )
        .with_event_wal(event_wal),
    );
    (store, data_root, lifecycle)
}

/// One complete repair→write→read round-trip through the unified
/// trait: a heal-style payload is written through the store, read back
/// through a SECOND trait-object handle (the same `.dat` the node's
/// other subsystems read), verified, overwritten with a corrected
/// payload (the heal rewrite), re-read, and finally deleted — the
/// scrub/AE/GC data-access lifecycle on one store.
#[tokio::test]
async fn repair_write_read_roundtrip_through_unified_trait() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_root, lifecycle) = pools_store(&tmp).await;
    // A second Arc to the SAME store — modeling the node's shared store
    // handed to heal (writer) and scrub/AE (readers).
    let reader_handle: Arc<dyn SegmentDataStore> = store.clone();

    let segment_id = SegmentId::new();
    let repaired: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();

    // The repair write happens under a lifecycle reservation (ADR-0032
    // D3 — the re-rep/push flows reserve before writing; heal targets
    // already-registered segments).
    lifecycle.request_reserve(segment_id, oceanfs_core::SizeTier::Standard, 4, 2).await.unwrap();
    let merkle_root = oceanfs_core::HashOutput::from_bytes(*blake3::hash(&repaired).as_bytes());
    lifecycle
        .request_seal(
            segment_id,
            oceanfs_core::SegmentMetadata {
                pool_id: 0,
                segment_id,
                ec_k: 4,
                ec_m: 2,
                size_tier: oceanfs_core::SizeTier::Standard,
                merkle_root: Some(merkle_root),
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1_700_000_000_000),
            },
            None,
        )
        .await
        .unwrap();

    // Repair write (heal/worker.rs execute_heal shape).
    store.write_segment_data(&segment_id, &repaired).await.expect("repair write");

    // Verify through the reader handle: parsed v1 header + exact data.
    let file = reader_handle
        .read_segment_data(&segment_id)
        .await
        .expect("read ok")
        .expect("segment present after write");
    assert_eq!(file.segment_id, segment_id);
    assert_eq!(file.version, 1);
    assert_eq!(file.header_len, 76);
    assert_eq!(&file.data[..], &repaired[..], "repair payload round-trips");

    // The file physically landed on the data pool root with a valid
    // v1 header (the strict read-path verification accepts it).
    let on_disk = std::fs::read(data_root.join(format!("{segment_id}.dat"))).unwrap();
    let header = oceanfs_storage::SegmentHeader::from_bytes(&on_disk).expect("valid header");
    assert_eq!(header.data_end() as usize, 76 + repaired.len());

    // Heal rewrite: an overwrite replaces the payload wholesale (the
    // atomic temp+rename path — never a partial file).
    let corrected: Vec<u8> = (0..4096u32).map(|i| (i % 253) as u8).collect();
    store.write_segment_data(&segment_id, &corrected).await.expect("heal rewrite");
    let file = store
        .read_segment_data(&segment_id)
        .await
        .expect("read ok")
        .expect("segment present after rewrite");
    assert_eq!(&file.data[..], &corrected[..], "rewrite replaces the payload");

    // Deletion reclaims the file bytes (header + data); a second read
    // is Ok(None) — the NotFound contract scrub/heal rely on.
    let reclaimed = store.delete_shards(&segment_id).await.expect("delete");
    assert_eq!(reclaimed, 76 + corrected.len() as u64, "header + data reclaimed");
    assert!(store.read_segment_data(&segment_id).await.expect("read ok").is_none());
    assert!(!data_root.join(format!("{segment_id}.dat")).exists());
}

/// The write-before-register bridge is GONE (ADR-0032 D3): a write to
/// an unregistered segment is rejected — every writer reserves first.
#[tokio::test]
async fn unregistered_write_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _data_root, _lifecycle) = pools_store(&tmp).await;
    let unmapped = SegmentId::new();
    let err = store
        .write_segment_data(&unmapped, b"replica payload")
        .await
        .expect_err("write-before-register must be rejected (ADR-0032 D3)");
    assert!(err.to_string().contains("not registered"), "{err}");
}
