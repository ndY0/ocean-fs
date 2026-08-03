//! Integration test: pipeline parallelism and segment pool.
//!
//! Tests pool rotation, concurrent writes, and encoding queue behavior.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

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

// ── Shard integration tests ──────────────────────────────────────

#[test]
fn shard_concurrent_writes_across_multiple_connection_ids() {
    use oceanfs_storage::segment::SegmentShard;

    let config = SegmentSizeConfig::default();
    let pool = BufferPool::new(65536, 8);
    let shard = Arc::new(SegmentShard::new(8, SizeTier::Standard, &config, &pool).unwrap());

    let counter = Arc::new(AtomicUsize::new(0));
    let num_threads = 8;
    let writes_per_thread = 20;

    let mut handles = vec![];
    for thread_id in 0..num_threads {
        let shard = Arc::clone(&shard);
        let counter = Arc::clone(&counter);
        let h = thread::spawn(move || {
            for i in 0..writes_per_thread {
                // Each thread uses a different connection_id to spread load.
                let conn_id = (thread_id as u64) * 1000 + i as u64;
                let mut seg = shard.get(conn_id);
                let result = seg.append(b"shard-write");
                assert!(result.is_ok(), "shard append must succeed");
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(counter.load(Ordering::Relaxed), num_threads * writes_per_thread);
}

#[test]
fn shard_routing_determinism_across_same_connection_id() {
    use oceanfs_storage::segment::SegmentShard;

    let config = SegmentSizeConfig::default();
    let pool = BufferPool::new(65536, 4);
    let shard = SegmentShard::new(4, SizeTier::Standard, &config, &pool).unwrap();

    // Same connection_id always routes to the same segment.
    let id1 = { shard.get(42).id() };
    let id2 = { shard.get(42).id() };
    assert_eq!(id1, id2, "same conn_id must route to same segment");
}

#[test]
fn shard_segment_fills_independently() {
    use oceanfs_storage::segment::SegmentShard;

    // Use a tiny target so a single large append fills the segment.
    let config = SegmentSizeConfig {
        default_target_size: 100,
        small_target_size: 100,
        ..SegmentSizeConfig::default()
    };
    let pool = BufferPool::new(65536, 8);
    let shard = SegmentShard::new(4, SizeTier::Standard, &config, &pool).unwrap();

    // Find two connection IDs that route to different segments.
    let seg_id_a = { shard.get(0).id() };
    let mut conn_id_b = 1u64;
    loop {
        let seg_id = { shard.get(conn_id_b).id() };
        if seg_id != seg_id_a {
            break;
        }
        conn_id_b += 1;
        // Safety belt: all 4 shards map to same segment → extremely unlikely.
        assert!(conn_id_b < 1000, "could not find different shard for conn_id");
    }

    // Fill shard A's segment.
    let large_data = vec![b'x'; 500];
    {
        let mut seg = shard.get(0);
        seg.append(&large_data).unwrap();
        assert!(seg.is_full(), "segment A should be full after {}-byte append", large_data.len());
    }

    // Shard B's segment should still accept writes.
    {
        let mut seg = shard.get(conn_id_b);
        let result = seg.append(b"small");
        assert!(result.is_ok(), "other shard (conn_id={conn_id_b}) should still accept writes");
    }
}
