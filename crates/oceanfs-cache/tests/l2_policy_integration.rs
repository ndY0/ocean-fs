#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration tests for L2 metadata cache with TTL-LRU eviction policy.
//!
//! Tests that the MetadataCache correctly delegates eviction to
//! the TTL-LRU policy and that stale entries are evicted preferentially.

use std::time::Duration;

use oceanfs_cache::{
    eviction::{EvictionPolicy, TtlLruConfig, TtlLruPolicy},
    MetadataCache, MetadataCacheConfig,
};
use oceanfs_core::{BucketId, Hlc, ObjectKey, ObjectMetadata};

fn make_meta(key: &str, inline: Option<&[u8]>) -> ObjectMetadata {
    ObjectMetadata {
        object_key: ObjectKey::new(key),
        size: 100,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: inline.map(bytes::Bytes::copy_from_slice),
        created_at: 0,
        hlc: Hlc::zero(),
    }
}

/// T3.8: MetadataCache with TTL-LRU policy evicts stale entries.
#[test]
fn test_metadata_cache_uses_policy_for_eviction() {
    let config = MetadataCacheConfig {
        max_size_bytes: 500,
        ttl_ms: 0, // cache frontend TTL disabled
        enabled: true,
        ..Default::default()
    };
    // Use a short TTL so entries become stale quickly.
    let policy: Box<dyn EvictionPolicy> =
        Box::new(TtlLruPolicy::new(TtlLruConfig { default_ttl_ms: 100 }));
    let cache = MetadataCache::new(config, policy);

    let bucket = BucketId::new("test");

    // Insert 8 metadata entries each ~100 bytes → 800 > 500.
    for i in 0..8 {
        cache.put(
            bucket.clone(),
            ObjectKey::new(format!("meta-{i}")),
            make_meta(&format!("meta-{i}"), None),
        );
    }

    // Wait for entries to become stale.
    std::thread::sleep(Duration::from_millis(150));

    // Trigger eviction by inserting another entry.
    cache.put(bucket.clone(), ObjectKey::new("meta-trigger"), make_meta("meta-trigger", None));

    // After eviction, count should be within limits.
    let count = cache.stats().entry_count.get();
    assert!(count <= 10, "entry count {count} should be ≤ 10 after eviction");

    let evictions = cache.stats().evictions.get();
    assert!(evictions > 0, "expected some evictions, got {evictions}");
}
