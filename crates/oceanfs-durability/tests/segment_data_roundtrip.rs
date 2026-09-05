//! Integration test: the migrated `SegmentDataStore` round-trip.
//!
//! Store-unification f1 (ADR-0032 D1): every durability consumer now
//! speaks the unified `oceanfs_storage_api::SegmentDataStore` trait.
//! This test drives one complete repair-style round-trip — write →
//! read → overwrite (the heal/re-rep shape: fetch a corrected payload,
//! persist it, verify it) — through a trait-object typed
//! `DiskSegmentStore` over a real pools-only layout, exercising the
//! NotFound contract (`Ok(None)`) and the delete path end to end.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::SegmentId;
use oceanfs_durability::DiskSegmentStore;
use oceanfs_storage_api::SegmentDataStore;

/// A pools-only store over one data pool (config-order id 0) plus the
/// mandatory wal/metadata/hints siblings — the node's boot shape since
/// ADR-0031 (pools mandatory).
fn pools_store(tmp: &tempfile::TempDir) -> (Arc<dyn SegmentDataStore>, std::path::PathBuf) {
    let data_root = tmp.path().join("nvme0");
    let storage = oceanfs_core::StorageConfig {
        pools: vec![
            oceanfs_core::StoragePoolConfig {
                name: "pool-a".into(),
                role: oceanfs_core::PoolRole::Data,
                root: data_root.clone(),
                weight: Some(1),
                tech: oceanfs_core::PoolTech::Auto,
                health: Default::default(),
            },
            oceanfs_core::StoragePoolConfig {
                name: "journal".into(),
                role: oceanfs_core::PoolRole::Wal,
                root: tmp.path().join("optane0"),
                weight: Some(1),
                tech: oceanfs_core::PoolTech::Auto,
                health: Default::default(),
            },
            oceanfs_core::StoragePoolConfig {
                name: "meta".into(),
                role: oceanfs_core::PoolRole::Metadata,
                root: tmp.path().join("optane1"),
                weight: Some(1),
                tech: oceanfs_core::PoolTech::Auto,
                health: Default::default(),
            },
            oceanfs_core::StoragePoolConfig {
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
    let registry = oceanfs_storage::PoolRegistry::from_config(&storage, &tmp.path().join("data"))
        .expect("registry");
    let pools = registry.data_pools();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0].id(), 0, "config-order id");
    let store: Arc<dyn SegmentDataStore> =
        Arc::new(DiskSegmentStore::new(pools, Arc::new(|_| Some(0))));
    (store, data_root)
}

/// One complete repair→write→read round-trip through the migrated
/// trait: a heal-style payload is written through the store, read back
/// through a SECOND trait-object handle (the same `.dat` the node's
/// other subsystems read), verified, overwritten with a corrected
/// payload (the heal rewrite), re-read, and finally deleted — the
/// scrub/AE/GC data-access lifecycle on one store.
#[tokio::test]
async fn repair_write_read_roundtrip_through_unified_trait() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_root) = pools_store(&tmp);
    // A second Arc to the SAME disk impl — modeling the node's shared
    // store handed to heal (writer) and scrub/AE (readers).
    let reader_handle: Arc<dyn SegmentDataStore> = store.clone();

    let segment_id = SegmentId::new();
    let repaired: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();

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

    // Heal rewrite: an overwrite replaces the payload wholesale.
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

/// The write-before-register bridge (f2 removes it): an unmapped
/// segment still resolves to pool 0, so the push/re-rep flows keep
/// working through the migrated trait during the transition window.
#[tokio::test]
async fn unmapped_segment_write_lands_on_first_pool() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, data_root) = pools_store(&tmp);
    let unmapped = SegmentId::new();
    store
        .write_segment_data(&unmapped, b"replica payload")
        .await
        .expect("write-before-register write");
    assert!(data_root.join(format!("{unmapped}.dat")).exists());
    let file = store.read_segment_data(&unmapped).await.unwrap().expect("present");
    assert_eq!(&file.data[..], b"replica payload");
}
