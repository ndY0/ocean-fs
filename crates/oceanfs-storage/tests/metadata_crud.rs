#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: MetadataStore CRUD operations.
//!
//! Verifies object, segment, and tombstone CRUD against RocksDB.
//! Covers the `rocksdb-metadata-store` feature's Definition of Done:
//! - put/get/delete object round-trip
//! - inline blob round-trip
//! - segment metadata round-trip
//! - tombstone round-trip
//! - list_objects with prefix
//! - batch atomic writes

use oceanfs_core::{
    BucketId, ChunkRef, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SegmentId, SegmentMetadata,
    SizeTier, Tombstone,
};
use oceanfs_storage::{BatchOp, MetadataStore};

fn test_bucket() -> BucketId {
    // The current MetadataStore::put_object hardcodes "default" as the bucket
    // prefix. Use "default" here until multi-bucket support is added.
    BucketId::new("default")
}

fn test_key(name: &str) -> ObjectKey {
    ObjectKey::new(name)
}

fn make_store(dir: &tempfile::TempDir) -> MetadataStore {
    let config = MetadataConfig {
        data_dir: dir.path().join("meta"),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 64 * 1024 * 1024,
    };
    MetadataStore::open(&config).expect("failed to open MetadataStore")
}

// ---------------------------------------------------------------------------
// Object CRUD
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_object_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let meta = ObjectMetadata {
        object_key: test_key("photo.jpg"),
        size: 1024,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(bytes::Bytes::from_static(b"fake-image-data")),
        created_at: 1_700_000_000_000,
        hlc: Hlc::zero(),
    };

    store.put_object(meta.clone()).expect("put_object failed");

    let fetched = store
        .get_object(&test_bucket(), &test_key("photo.jpg"))
        .expect("get_object failed")
        .expect("object not found");

    assert_eq!(fetched.object_key.as_str(), "photo.jpg");
    assert_eq!(fetched.size, 1024);
    assert!(fetched.inline_data.is_some());
    assert_eq!(fetched.inline_data.as_deref(), Some(&b"fake-image-data"[..]));
}

#[test]
fn get_nonexistent_object_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let result =
        store.get_object(&test_bucket(), &test_key("no-such-key")).expect("get_object failed");
    assert!(result.is_none());
}

#[test]
fn delete_object_removes_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let meta = ObjectMetadata {
        object_key: test_key("tmp.dat"),
        size: 64,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(bytes::Bytes::from_static(b"tmp")),
        created_at: 1_700_000_000_000,
        hlc: Hlc::zero(),
    };
    store.put_object(meta).unwrap();

    // Confirm it exists.
    assert!(store.get_object(&test_bucket(), &test_key("tmp.dat")).unwrap().is_some());

    store.delete_object(&test_bucket(), &test_key("tmp.dat")).unwrap();

    // Confirm it is gone.
    assert!(store.get_object(&test_bucket(), &test_key("tmp.dat")).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Segment metadata CRUD
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_segment_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let seg_id = SegmentId::new();
    let meta = SegmentMetadata {
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1_700_000_000_000),
    };

    store.put_segment(meta).expect("put_segment failed");

    let fetched =
        store.get_segment(seg_id).expect("get_segment failed").expect("segment not found");

    assert_eq!(fetched.segment_id.as_uuid(), seg_id.as_uuid());
    assert_eq!(fetched.ec_k, 4);
    assert_eq!(fetched.ec_m, 2);
    assert!(fetched.is_sealed());
}

#[test]
fn get_nonexistent_segment_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let result = store.get_segment(SegmentId::new()).unwrap();
    assert!(result.is_none());
}

// ---------------------------------------------------------------------------
// Tombstone CRUD
// ---------------------------------------------------------------------------

#[test]
fn tombstone_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let tombstone = Tombstone { deletion_time: 1_700_000_000_000, hlc: Hlc::zero() };

    store.put_tombstone(&test_bucket(), &test_key("deleted-key"), tombstone).unwrap();

    assert!(store.has_tombstone(&test_bucket(), &test_key("deleted-key")).unwrap());
}

#[test]
fn no_tombstone_for_nonexistent_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    assert!(!store.has_tombstone(&test_bucket(), &test_key("alive-key")).unwrap());
}

// ---------------------------------------------------------------------------
// List objects with prefix
// ---------------------------------------------------------------------------

#[test]
fn list_objects_by_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    // Insert several objects with different prefixes.
    for name in &["a/1.txt", "a/2.txt", "b/1.txt"] {
        let meta = ObjectMetadata {
            object_key: ObjectKey::new(*name),
            size: 10,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(bytes::Bytes::from_static(b"x")),
            created_at: 1_700_000_000_000,
            hlc: Hlc::zero(),
        };
        store.put_object(meta).unwrap();
    }

    // List with prefix "a/"
    let results = store.list_objects(&test_bucket(), "a/");
    let count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(count, 2, "expected 2 objects with prefix 'a/'");

    // List with prefix "b/"
    let results = store.list_objects(&test_bucket(), "b/");
    let count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(count, 1, "expected 1 object with prefix 'b/'");
}

// ---------------------------------------------------------------------------
// Inline blob round-trip
// ---------------------------------------------------------------------------

#[test]
fn inline_blob_roundtrip_across_crud() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let payload = bytes::Bytes::from(vec![0xAB; 4096]); // exactly at inline threshold
    let meta = ObjectMetadata {
        object_key: test_key("inline-blob"),
        size: 4096,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(payload.clone()),
        created_at: 1_700_000_000_000,
        hlc: Hlc::zero(),
    };
    assert!(meta.is_inline());
    assert!(!meta.is_segment_stored());

    store.put_object(meta).unwrap();

    let fetched = store
        .get_object(&test_bucket(), &test_key("inline-blob"))
        .unwrap()
        .expect("object not found");

    assert!(fetched.is_inline());
    assert_eq!(fetched.inline_data.as_deref(), Some(&payload[..]));
}

// ---------------------------------------------------------------------------
// Chunk-referenced object round-trip
// ---------------------------------------------------------------------------

#[test]
fn chunk_stored_object_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let seg_id = SegmentId::new();
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(ChunkRef { segment_id: seg_id, offset: 0, length: 500 });

    let meta = ObjectMetadata {
        object_key: test_key("chunked-blob"),
        size: 500,
        blake3_hash: None,
        chunks,
        inline_data: None,
        created_at: 1_700_000_000_000,
        hlc: Hlc::zero(),
    };
    assert!(!meta.is_inline());
    assert!(meta.is_segment_stored());

    store.put_object(meta).unwrap();

    let fetched = store
        .get_object(&test_bucket(), &test_key("chunked-blob"))
        .unwrap()
        .expect("object not found");

    assert!(!fetched.is_inline());
    assert!(fetched.is_segment_stored());
    assert_eq!(fetched.chunks.len(), 1);
    assert_eq!(fetched.chunks[0].segment_id.as_uuid(), seg_id.as_uuid());
    assert_eq!(fetched.chunks[0].offset, 0);
    assert_eq!(fetched.chunks[0].length, 500);
}

// ---------------------------------------------------------------------------
// Batch atomic writes
// ---------------------------------------------------------------------------

#[test]
fn batch_write_atomicity() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let meta = ObjectMetadata {
        object_key: test_key("batch-obj"),
        size: 32,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(bytes::Bytes::from_static(b"batch-data")),
        created_at: 1_700_000_000_000,
        hlc: Hlc::zero(),
    };

    let tombstone = Tombstone { deletion_time: 1_700_000_001_000, hlc: Hlc::zero() };

    let ops = vec![
        BatchOp::PutObject(test_key("batch-obj"), meta),
        BatchOp::PutTombstone(test_bucket(), test_key("batch-obj"), tombstone),
    ];

    store.batch_write(ops).expect("batch_write failed");

    // Object should exist.
    assert!(store.get_object(&test_bucket(), &test_key("batch-obj")).unwrap().is_some());

    // Tombstone should exist.
    assert!(store.has_tombstone(&test_bucket(), &test_key("batch-obj")).unwrap());
}

#[test]
fn batch_write_no_ops_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    // Empty batch should succeed.
    store.batch_write(vec![]).expect("empty batch should succeed");
}
