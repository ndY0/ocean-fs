//! Per-core segment sharding for write concurrency.
//!
//! Routes incoming writes to one of N independent active segment groups
//! by hashing the connection or request ID. This reduces lock contention
//! on the active segment's append path by a factor of `shard_count`.
//!
//! Per performance guideline §2.5: sharded segment buffer per worker thread.

use oceanfs_core::{SegmentSizeConfig, SizeTier};

use crate::{buffer_pool::BufferPool, error::Result, segment::buffer::ActiveSegment};

/// Routes writes to one of N active segment groups by hashing.
///
/// Each shard group maintains its own active segment pool, isolating
/// concurrent writes and reducing append-lock contention.
///
/// # Examples
///
/// ```ignore
/// // SegmentShard is pub(crate); examples are in unit tests.
/// use oceanfs_core::{SegmentSizeConfig, SizeTier};
/// use oceanfs_storage::BufferPool;
/// use oceanfs_storage::segment::shard::SegmentShard;
///
/// let config = SegmentSizeConfig::default();
/// let pool = BufferPool::new(65536, 8);
/// let shard = SegmentShard::new(4, SizeTier::Standard, &config, &pool).unwrap();
///
/// // Different connection IDs may route to different segments.
/// let seg_a = shard.get(42);
/// let seg_b = shard.get(99);
/// ```
#[allow(dead_code)]
pub struct SegmentShard {
    /// The active segments, one per shard.
    segments: Vec<parking_lot::Mutex<ActiveSegment>>,
    /// Number of shards.
    shard_count: usize,
}

#[allow(dead_code)]
impl SegmentShard {
    /// Creates a new `SegmentShard` with `count` independent segment groups.
    ///
    /// Each group is initialized with a fresh active segment from the pool.
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `count` is zero.
    ///
    /// # Errors
    ///
    /// Returns [`Error::BufferPoolExhausted`] if the buffer pool cannot
    /// provide enough buffers.
    pub fn new(
        count: usize,
        tier: SizeTier,
        config: &SegmentSizeConfig,
        pool: &BufferPool,
    ) -> Result<Self> {
        debug_assert!(count > 0, "shard count must be > 0");

        // Pre-size the vector.
        let mut segments = Vec::with_capacity(count);
        for _ in 0..count {
            let seg = ActiveSegment::new(tier, config, pool)?;
            segments.push(parking_lot::Mutex::new(seg));
        }

        Ok(Self { segments, shard_count: count })
    }

    /// Returns a reference to the active segment for the given connection ID.
    ///
    /// The segment is selected deterministically: `connection_id % shard_count`.
    /// The returned `MutexGuard` must be dropped before calling `get` again
    /// with a connection ID that hashes to the same shard, or a deadlock
    /// will occur (this `Mutex` is not reentrant).
    pub fn get(&self, connection_id: u64) -> parking_lot::MutexGuard<'_, ActiveSegment> {
        let index = (connection_id as usize) % self.shard_count;
        self.segments[index].lock()
    }

    /// Returns the number of shard groups.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn config() -> SegmentSizeConfig {
        SegmentSizeConfig::default()
    }

    fn pool() -> BufferPool {
        BufferPool::new(65536, 8)
    }

    #[test]
    fn shard_routes_different_ids_to_different_segments() {
        let shard = SegmentShard::new(4, SizeTier::Standard, &config(), &pool()).unwrap();

        // Check that different IDs route to different indices when
        // their mod results differ.
        let seg0 = shard.get(0);
        let seg3 = shard.get(3);
        assert_ne!(seg0.id(), seg3.id());
    }

    #[test]
    fn shard_routes_same_mod_to_same_segment() {
        let shard = SegmentShard::new(4, SizeTier::Standard, &config(), &pool()).unwrap();

        let seg0_id = {
            let seg0 = shard.get(0);
            seg0.id()
        };
        let seg4_id = {
            let seg4 = shard.get(4);
            seg4.id()
        };
        assert_eq!(seg0_id, seg4_id);
    }

    #[test]
    fn shard_distributes_writes_across_all_groups() {
        let shard = SegmentShard::new(4, SizeTier::Standard, &config(), &pool()).unwrap();

        let mut ids = std::collections::HashSet::new();
        for conn_id in 0..4u64 {
            let seg = shard.get(conn_id);
            ids.insert(seg.id());
        }
        // All 4 shards should have different segment IDs.
        assert_eq!(ids.len(), 4);
    }

    #[test]
    fn shard_count_matches_config() {
        let shard = SegmentShard::new(8, SizeTier::Standard, &config(), &pool()).unwrap();
        assert_eq!(shard.shard_count(), 8);
    }

    #[test]
    #[should_panic]
    fn zero_shard_count_panics() {
        SegmentShard::new(0, SizeTier::Standard, &config(), &pool()).unwrap();
    }
}
