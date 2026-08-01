//! Integration tests for the caching layer.
//!
//! Exercises L1 (object), L2 (metadata), L3 (negative) caches
//! and the prefetch engine together.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_cache::{
    MetadataCache, MetadataCacheConfig, NegativeCache, NegativeCacheConfig, ObjectCache,
    ObjectCacheConfig, PrefetchConfig, PrefetchEngine,
};
use oceanfs_core::{BucketId, Hlc, MetadataStore, ObjectKey, ObjectMetadata};

/// A mock metadata store for integration testing.
struct MockStore {
    entries: Vec<(BucketId, ObjectKey, ObjectMetadata)>,
}

impl MockStore {
    fn new(entries: Vec<(BucketId, ObjectKey, ObjectMetadata)>) -> Self {
        Self { entries }
    }
}

impl MetadataStore for MockStore {
    fn list_object_keys(
        &self,
        bucket: &BucketId,
    ) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
        Ok(self
            .entries
            .iter()
            .filter(|(b, _, _)| b == bucket)
            .map(|(b, k, _)| (b.clone(), k.clone()))
            .collect())
    }

    fn get_object_metadata(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> std::io::Result<Option<ObjectMetadata>> {
        Ok(self
            .entries
            .iter()
            .find(|(b, k, _)| b == bucket && k == key)
            .map(|(_, _, m)| m.clone()))
    }
}

fn make_meta(key: &str, inline: Option<&[u8]>) -> ObjectMetadata {
    ObjectMetadata {
        object_key: ObjectKey::new(key),
        size: (inline.map(|d| d.len()).unwrap_or(0)) as u64,
        blake3_hash: None,
        chunks: if inline.is_some() {
            smallvec::SmallVec::new()
        } else {
            let mut chunks = smallvec::SmallVec::new();
            chunks.push(oceanfs_core::ChunkRef {
                segment_id: oceanfs_core::SegmentId::new(),
                offset: 0,
                length: 1024,
            });
            chunks
        },
        inline_data: inline.map(Bytes::copy_from_slice),
        created_at: 0,
        hlc: Hlc::zero(),
    }
}

#[test]
fn l1_cache_hit_miss_scenario() {
    let cache = ObjectCache::new(ObjectCacheConfig::default());
    let bucket = BucketId::new("photos");
    let key = ObjectKey::new("sunset.jpg");
    let data = Bytes::from_static(b"image data here");

    // First access: miss.
    assert!(cache.get(&bucket, &key).is_none());

    // Insert and access: hit.
    cache.put(bucket.clone(), key.clone(), data.clone());
    assert_eq!(cache.get(&bucket, &key), Some(data));

    // Invalidate: miss.
    cache.invalidate(&bucket, &key);
    assert!(cache.get(&bucket, &key).is_none());
}

#[test]
fn l2_cache_inline_serving() {
    let cache = MetadataCache::new(MetadataCacheConfig::default());
    let bucket = BucketId::new("data");
    let key = ObjectKey::new("small.txt");

    // Insert metadata with inline data.
    let meta = make_meta("small.txt", Some(b"inline content"));
    cache.put(bucket.clone(), key.clone(), meta);

    // Get: should be a hit with inline data.
    let hit = cache.get(&bucket, &key).unwrap();
    assert!(hit.is_inline());
    assert_eq!(hit.inline_data, Some(Bytes::from_static(b"inline content")));
    assert_eq!(
        cache.stats().inline_hits.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn l3_negative_cache_filters_nonexistent() {
    let cache = NegativeCache::new(NegativeCacheConfig {
        enabled: true,
        size_bytes: 1024,
        fp_rate: 0.01,
        rebuild_interval_sec: 3600,
    });
    let bucket = BucketId::new("archive");
    let existing = ObjectKey::new("exists.txt");
    let missing = ObjectKey::new("missing.txt");

    // Insert existing key.
    cache.insert(&bucket, &existing);

    // Key exists: filter says "maybe".
    assert!(cache.contains(&bucket, &existing));

    // Key missing: filter says "definitely not".
    assert!(!cache.contains(&bucket, &missing));

    // Hit counter should be incremented for definite-absent check.
    assert!(cache.stats().hits.load(std::sync::atomic::Ordering::Relaxed) > 0);
}

#[test]
fn l1_l2_cascade_scenario() {
    // Simulate read path: L1 miss → L2 hit with inline data.
    let l1 = ObjectCache::new(ObjectCacheConfig::default());
    let l2 = MetadataCache::new(MetadataCacheConfig::default());
    let bucket = BucketId::new("b");
    let key = ObjectKey::new("obj");

    // L2 has inline metadata.
    l2.put(
        bucket.clone(),
        key.clone(),
        make_meta("obj", Some(b"cached-data")),
    );

    // L1 miss: check L1 first.
    assert!(l1.get(&bucket, &key).is_none());

    // L2 hit with inline data.
    let meta = l2.get(&bucket, &key).unwrap();
    assert!(meta.is_inline());
    assert_eq!(
        meta.inline_data,
        Some(Bytes::from_static(b"cached-data"))
    );
}

#[test]
fn negative_cache_rebuild_from_store() {
    let bucket = BucketId::new("test-bucket");
    let entries = vec![
        (
            bucket.clone(),
            ObjectKey::new("a"),
            make_meta("a", None),
        ),
        (
            bucket.clone(),
            ObjectKey::new("b"),
            make_meta("b", None),
        ),
    ];
    let store = Arc::new(MockStore::new(entries));

    let cache = NegativeCache::new(NegativeCacheConfig {
        enabled: true,
        size_bytes: 1024 * 1024,
        fp_rate: 0.0001,
        rebuild_interval_sec: 3600,
    });

    // Before rebuild, a key is definitely absent.
    assert!(!cache.contains(&bucket, &ObjectKey::new("a")));

    // Rebuild from the store.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cache.rebuild(store as Arc<dyn MetadataStore>).await.unwrap();
    });

    // After rebuild, existing keys are "maybe present".
    assert!(cache.contains(&bucket, &ObjectKey::new("a")));
    assert!(cache.contains(&bucket, &ObjectKey::new("b")));

    // Non-existing key still "definitely absent".
    assert!(!cache.contains(&bucket, &ObjectKey::new("c")));
}

#[tokio::test]
async fn prefetch_warms_metadata_cache() {
    let bucket = BucketId::new("photos");
    let entries = vec![
        (
            bucket.clone(),
            ObjectKey::new("img-001.jpg"),
            make_meta("img-001.jpg", Some(b"data1")),
        ),
        (
            bucket.clone(),
            ObjectKey::new("img-002.jpg"),
            make_meta("img-002.jpg", Some(b"data2")),
        ),
    ];
    let store: Arc<dyn MetadataStore> = Arc::new(MockStore::new(entries));
    let metadata_cache = Arc::new(MetadataCache::new(MetadataCacheConfig::default()));
    let object_cache = Arc::new(ObjectCache::new(ObjectCacheConfig::default()));

    let engine = PrefetchEngine::new(
        PrefetchConfig {
            enabled: true,
            after_list: 10,
            max_concurrency: 4,
            queue_capacity: 32,
            ..Default::default()
        },
        metadata_cache.clone(),
        Some(object_cache.clone()),
        store,
    );

    // Simulate LIST response prefetch.
    let keys = [
        ObjectKey::new("img-001.jpg"),
        ObjectKey::new("img-002.jpg"),
    ];
    engine.after_list(bucket.clone(), &keys, 0);

    // Allow worker to process.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Metadata cache should be warm.
    assert!(metadata_cache
        .get(&bucket, &ObjectKey::new("img-001.jpg"))
        .is_some());
    assert!(metadata_cache
        .get(&bucket, &ObjectKey::new("img-002.jpg"))
        .is_some());

    // Object cache should have inline blobs.
    assert_eq!(
        object_cache.get(&bucket, &ObjectKey::new("img-001.jpg")),
        Some(Bytes::from_static(b"data1"))
    );
}

#[test]
fn stats_accumulate_over_operations() {
    let cache = ObjectCache::new(ObjectCacheConfig::default());
    let bucket = BucketId::new("stats-bucket");
    let key = ObjectKey::new("stats-key");

    // Miss.
    cache.get(&bucket, &key);
    assert_eq!(
        cache.stats().misses.load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    // Put + get = hit.
    cache.put(bucket.clone(), key.clone(), Bytes::from_static(b"data"));
    cache.get(&bucket, &key);
    assert_eq!(
        cache.stats().hits.load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    // Hit rate.
    let rate = cache.stats().hit_rate();
    assert!((rate - 0.5).abs() < 0.01);
}
