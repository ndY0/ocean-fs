//! Cache behavior integration tests.
//!
//! Verifies L1 object cache, L2 metadata cache, and L3 negative
//! cache hit/miss behavior and invalidation.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use oceanfs_cache::{
    eviction::{GdsfConfig, GdsfPolicy, TtlLruConfig, TtlLruPolicy},
    MetadataCache, MetadataCacheConfig, NegativeCache, NegativeCacheConfig, ObjectCache,
    ObjectCacheConfig,
};
use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata};

fn make_meta_cache(config: MetadataCacheConfig) -> MetadataCache {
    MetadataCache::new(config, Box::new(TtlLruPolicy::new(TtlLruConfig::default())))
}

fn make_bucket() -> BucketId {
    BucketId::new("test-bucket")
}

fn make_key(name: &str) -> ObjectKey {
    ObjectKey::new(name)
}

fn make_obj_cache(config: ObjectCacheConfig) -> ObjectCache {
    ObjectCache::new(config, Box::new(GdsfPolicy::new(GdsfConfig::default())))
}

#[test]
fn l1_cache_put_and_get_returns_data() {
    let cache = make_obj_cache(ObjectCacheConfig {
        enabled: true,
        max_size_bytes: 64 * 1024,
        ttl_ms: 60_000,
        max_blob_size: 1024 * 1024,
        ..Default::default()
    });

    let bucket = make_bucket();
    let key = make_key("hello.txt");
    let data = Bytes::from_static(b"hello l1 cache");

    cache.put(bucket.clone(), key.clone(), data.clone());
    let result = cache.get(&bucket, &key);
    assert_eq!(result, Some(data));
}

#[test]
fn l1_cache_miss_returns_none() {
    let cache = make_obj_cache(ObjectCacheConfig {
        enabled: true,
        max_size_bytes: 64 * 1024,
        ttl_ms: 60_000,
        max_blob_size: 1024 * 1024,
        ..Default::default()
    });

    let result = cache.get(&make_bucket(), &make_key("nonexistent"));
    assert_eq!(result, None);
}

#[test]
fn l1_cache_invalidate_removes_entry() {
    let cache = make_obj_cache(ObjectCacheConfig {
        enabled: true,
        max_size_bytes: 64 * 1024,
        ttl_ms: 60_000,
        max_blob_size: 1024 * 1024,
        ..Default::default()
    });

    let bucket = make_bucket();
    let key = make_key("delete-me.txt");
    cache.put(bucket.clone(), key.clone(), Bytes::from_static(b"data"));

    // Should be present before invalidate.
    assert!(cache.get(&bucket, &key).is_some());

    cache.invalidate(&bucket, &key);
    assert_eq!(cache.get(&bucket, &key), None);
}

#[test]
fn l2_metadata_cache_put_and_get() {
    let cache = make_meta_cache(MetadataCacheConfig {
        enabled: true,
        max_size_bytes: 1024 * 1024,
        ttl_ms: 300_000,
        ..Default::default()
    });

    let bucket = make_bucket();
    let key = make_key("meta-test");
    let meta = ObjectMetadata {
        object_key: key.clone(),
        size: 42,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(Bytes::from_static(b"inline data")),
        created_at: 0,
        hlc: oceanfs_core::Hlc::zero(),
    };

    cache.put(bucket.clone(), key.clone(), meta.clone());
    let cached = cache.get(&bucket, &key);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().size, 42);
}

#[test]
fn l2_cache_miss_returns_none() {
    let cache = make_meta_cache(MetadataCacheConfig {
        enabled: true,
        max_size_bytes: 1024 * 1024,
        ttl_ms: 300_000,
        ..Default::default()
    });

    let result = cache.get(&make_bucket(), &make_key("not-cached"));
    assert!(result.is_none());
}

#[test]
fn l2_cache_invalidate_removes_entry() {
    let cache = make_meta_cache(MetadataCacheConfig {
        enabled: true,
        max_size_bytes: 1024 * 1024,
        ttl_ms: 300_000,
        ..Default::default()
    });

    let bucket = make_bucket();
    let key = make_key("l2-delete");
    let meta = ObjectMetadata {
        object_key: key.clone(),
        size: 10,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: None,
        created_at: 0,
        hlc: oceanfs_core::Hlc::zero(),
    };

    cache.put(bucket.clone(), key.clone(), meta);
    assert!(cache.get(&bucket, &key).is_some());

    cache.invalidate(&bucket, &key);
    assert!(cache.get(&bucket, &key).is_none());
}

#[test]
fn l3_negative_cache_insert_and_query() {
    let cache = NegativeCache::new(NegativeCacheConfig {
        enabled: true,
        size_bytes: 64 * 1024,
        fp_rate: 0.01,
        rebuild_interval_sec: 3600,
        ..Default::default()
    });

    let bucket = make_bucket();
    let key = make_key("test-key");

    // Insert the key into the filter (should not panic).
    cache.insert(&bucket, &key);

    // A key in the negative set is reported as definitely absent.
    assert!(cache.contains(&bucket, &key));
}

#[test]
fn l3_negative_cache_disabled_never_reports_absent() {
    let cache = NegativeCache::new(NegativeCacheConfig {
        enabled: false,
        size_bytes: 64 * 1024,
        fp_rate: 0.01,
        rebuild_interval_sec: 3600,
        ..Default::default()
    });

    // When disabled, the cache must never claim a key is absent —
    // callers fall through to the real metadata store.
    assert!(!cache.contains(&make_bucket(), &make_key("anything")));
}

#[test]
fn object_cache_stats_reflect_operations() {
    let cache = make_obj_cache(ObjectCacheConfig {
        enabled: true,
        max_size_bytes: 64 * 1024,
        ttl_ms: 60_000,
        max_blob_size: 1024 * 1024,
        ..Default::default()
    });

    let bucket = make_bucket();
    let key = make_key("stats.txt");
    let data = Bytes::from_static(b"stats test");

    cache.put(bucket.clone(), key.clone(), data);
    let _ = cache.get(&bucket, &key);
    let _ = cache.get(&bucket, &make_key("miss"));

    let stats = cache.stats();
    // After one put, we expect at least the entry_count to be 1.
    let count = stats.entry_count.get();
    assert!(count >= 1);
}
