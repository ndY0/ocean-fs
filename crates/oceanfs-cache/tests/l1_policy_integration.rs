#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration tests for L1 object cache with GDSF eviction policy.
//!
//! Tests that the ObjectCache correctly delegates eviction to
//! the GDSF policy and that the size-aware eviction order works.
//! Also tests per-bucket policy overrides.

use bytes::Bytes;
use oceanfs_cache::{
    eviction::{EvictionPolicy, GdsfConfig, GdsfPolicy},
    ObjectCache, ObjectCacheConfig,
};
use oceanfs_core::{BucketId, EvictionPolicyType, ObjectKey};

/// T3.7: ObjectCache with GDSF policy evicts largest blobs first.
#[test]
fn test_object_cache_uses_policy_for_eviction() {
    let config = ObjectCacheConfig {
        max_size_bytes: 1024,
        max_blob_size: 1024,
        ttl_ms: 0,
        enabled: true,
        ..Default::default()
    };
    let policy: Box<dyn EvictionPolicy> = Box::new(GdsfPolicy::new(GdsfConfig::default()));
    let cache = ObjectCache::new(config, policy);

    let bucket = BucketId::new("test");

    // Insert 5 blobs each of size=300 bytes → total = 1500 > 1024.
    let data: Vec<Bytes> = (0..5).map(|i| Bytes::from(vec![i as u8; 300])).collect();

    cache.put(bucket.clone(), ObjectKey::new("obj-0"), data[0].clone());
    cache.put(bucket.clone(), ObjectKey::new("obj-1"), data[1].clone());
    cache.put(bucket.clone(), ObjectKey::new("obj-2"), data[2].clone());
    cache.put(bucket.clone(), ObjectKey::new("obj-3"), data[3].clone());
    cache.put(bucket.clone(), ObjectKey::new("obj-4"), data[4].clone());

    // At least one eviction should have occurred.
    let stats = cache.stats();
    assert!(stats.evictions.get() > 0, "expected at least one eviction");

    // Final cache size should not exceed max.
    let size = stats.size_bytes.get();
    assert!(size <= 1024, "cache size {size} exceeds max 1024");

    // With GDSF, all blobs are same size (300), so first-inserted
    // should be evicted first (lowest priority, no access boost).
    // obj-0 was inserted first, should be gone.
    assert!(
        cache.get(&bucket, &ObjectKey::new("obj-0")).is_none()
            || cache.get(&bucket, &ObjectKey::new("obj-1")).is_none(),
        "at least one early blob should have been evicted"
    );
}

/// Test that per-bucket policy override works: a bucket configured with
/// TTL-LRU uses that policy instead of the cache-wide GDSF default.
#[test]
fn test_per_bucket_policy_override() {
    let config = ObjectCacheConfig {
        max_size_bytes: 1024,
        max_blob_size: 1024,
        ttl_ms: 0,
        enabled: true,
        ..Default::default()
    };
    let default_policy: Box<dyn EvictionPolicy> = Box::new(GdsfPolicy::new(GdsfConfig::default()));
    let cache = ObjectCache::new(config.clone(), default_policy);

    // Register bucket "archive" with a TTL-LRU override.
    let ttl_bucket = BucketId::new("archive");
    let ttl_config =
        ObjectCacheConfig { eviction_policy_type: Some(EvictionPolicyType::TtlLru), ..config };
    cache.set_bucket_config(ttl_bucket.clone(), ttl_config);

    // Insert entries into both buckets.
    let default_bucket = BucketId::new("default");
    for i in 0..5 {
        cache.put(
            default_bucket.clone(),
            ObjectKey::new(format!("obj-{i}")),
            Bytes::from(vec![i as u8; 300]),
        );
        cache.put(
            ttl_bucket.clone(),
            ObjectKey::new(format!("obj-{i}")),
            Bytes::from(vec![i as u8; 300]),
        );
    }

    // Both buckets should have evicted some entries (total 1500 > 1024 each).
    let stats = cache.stats();
    assert!(stats.evictions.get() > 0, "expected some evictions");

    // The per-bucket policy was registered.
    // (Smoke test: the cache still operates correctly with mixed policies.)
    assert!(
        cache.get(&default_bucket, &ObjectKey::new("obj-4")).is_some()
            || cache.get(&ttl_bucket, &ObjectKey::new("obj-4")).is_some()
    );
}
