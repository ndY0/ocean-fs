//! Integration test: pipeline parallelism and segment pool.
//!
//! Tests pool rotation, concurrent writes, and encoding queue behavior.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use oceanfs_core::{PoolConfig, SegmentSizeConfig, SizeTier};
use oceanfs_storage::BufferPool;

// The SegmentPool is pub(crate), so we test via the BufferPool + SegmentShard path.
// Direct pool tests are in the unit test module.

#[test]
fn pool_config_defaults_are_sensible() {
    let cfg = PoolConfig::default();
    assert_eq!(cfg.active_pool_size, 4);
    assert_eq!(cfg.shard_count, 4);
    assert_eq!(cfg.max_inflight_encodes, 8);
    assert_eq!(cfg.encode_queue_capacity, 64);
}

#[test]
fn pool_config_custom_sizes() {
    let cfg = PoolConfig {
        active_pool_size: 8,
        shard_count: 16,
        max_inflight_encodes: 32,
        encode_queue_capacity: 128,
    };
    assert_eq!(cfg.active_pool_size, 8);
    assert_eq!(cfg.shard_count, 16);
}

#[test]
fn buffer_pool_concurrent_acquire_release() {
    let pool = Arc::new(BufferPool::new(65536, 16));
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..8 {
        let pool = Arc::clone(&pool);
        let counter = Arc::clone(&counter);
        let h = thread::spawn(move || {
            for _ in 0..10 {
                let buf = pool.acquire().unwrap();
                // Simulate some work.
                thread::sleep(std::time::Duration::from_micros(10));
                pool.release(buf);
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(counter.load(Ordering::Relaxed), 80);
}

#[test]
fn tier_classification_boundaries() {
    let cfg = SegmentSizeConfig::default();
    assert_eq!(cfg.classify(1024), SizeTier::Inline);
    assert_eq!(cfg.classify(4096), SizeTier::Inline);
    assert_eq!(cfg.classify(4097), SizeTier::Small);
    assert_eq!(cfg.classify(262144), SizeTier::Small);
    assert_eq!(cfg.classify(262145), SizeTier::Standard);
    assert_eq!(cfg.classify(4194304), SizeTier::Standard);
    assert_eq!(cfg.classify(4194305), SizeTier::Multi);
}
