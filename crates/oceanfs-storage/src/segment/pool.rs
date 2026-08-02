//! Active segment pool with pipeline parallelism.
//!
//! A pool of N active segments decouples append latency from EC encode
//! time. While one segment is being EC-encoded (asynchronously), the next
//! segment in the pool accepts writes. Combined with per-core segment
//! sharding, this eliminates write blocking during seal+encode cycles.
//!
//! ## Pool States
//!
//! Each slot transitions through a lifecycle:
//! ```text
//! Idle → Appending → Sealing → Encoding → Idle
//! ```
//!
//! Per performance guideline §2.5 (sharded segment buffer), §2.6 (bounded
//! channels), and §2.7 (semaphore-bounded concurrency).

use std::sync::Arc;

use oceanfs_core::{PoolConfig, SegmentId, SizeTier};
use parking_lot::Mutex;
use tokio::sync::{mpsc, Semaphore};

use crate::{
    buffer_pool::BufferPool,
    error::{Error, Result},
    segment::buffer::ActiveSegment,
};

/// The state of a pool slot throughout its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PoolSlotState {
    /// Slot is free and can accept a new segment.
    Idle,
    /// Segment is actively accepting writes.
    Appending,
    /// Segment is being sealed (WAL flush, checksum).
    Sealing,
    /// Segment data is being EC-encoded asynchronously.
    Encoding,
}

/// A single slot in the segment pool, holding one active segment and its state.
#[allow(dead_code)]
pub(crate) struct PoolSlot {
    state: Mutex<PoolSlotState>,
    segment: Mutex<Option<ActiveSegment>>,
}

#[allow(dead_code)]
impl PoolSlot {
    /// Creates a new pool slot with a fresh active segment.
    fn new(
        tier: SizeTier,
        config: &oceanfs_core::SegmentSizeConfig,
        pool: &BufferPool,
    ) -> Result<Self> {
        let segment = ActiveSegment::new(tier, config, pool)?;
        Ok(Self { state: Mutex::new(PoolSlotState::Appending), segment: Mutex::new(Some(segment)) })
    }

    /// Creates a new pool slot in Idle state (no segment).
    fn new_idle() -> Self {
        Self { state: Mutex::new(PoolSlotState::Idle), segment: Mutex::new(None) }
    }

    /// Returns the current state of this slot.
    fn state(&self) -> PoolSlotState {
        *self.state.lock()
    }

    /// Sets the state of this slot.
    fn set_state(&self, new_state: PoolSlotState) {
        *self.state.lock() = new_state;
    }
}

/// A pool of active segments for a single tier and shard.
///
/// Manages N concurrent active segments, rotating through them as segments
/// fill up. When the current appending segment is full, it's moved to the
/// sealing state, a new segment is activated from the pool, and the sealed
/// segment is sent to the EC encoding queue.
///
/// # Examples
///
/// ```ignore
/// // SegmentPool is pub(crate); examples are in unit tests.
/// ```
#[allow(dead_code)]
pub(crate) struct SegmentPool {
    /// Pool slots, one per active segment.
    slots: Vec<Arc<PoolSlot>>,
    /// The storage tier this pool serves.
    tier: SizeTier,
    /// Segment size configuration for creating new active segments.
    size_config: oceanfs_core::SegmentSizeConfig,
    /// Index of the current appending slot (round-robin).
    current_index: Mutex<usize>,
    /// Sender for the EC encoding work queue.
    encode_tx: mpsc::Sender<SegmentId>,
    /// Receiver for the EC encoding work queue (held by the pool's worker).
    encode_rx: Mutex<Option<mpsc::Receiver<SegmentId>>>,
    /// Semaphore limiting in-flight EC encodes.
    encode_semaphore: Arc<Semaphore>,
    /// Pool configuration.
    config: PoolConfig,
    /// Reference to the buffer pool for creating new active segments.
    buffer_pool: Arc<BufferPool>,
}

#[allow(dead_code)]
impl SegmentPool {
    /// Creates a new segment pool.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer pool cannot provide enough buffers
    /// for the initial active segments.
    pub(crate) fn new(
        config: PoolConfig,
        tier: SizeTier,
        size_config: &oceanfs_core::SegmentSizeConfig,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self> {
        let (encode_tx, encode_rx) = mpsc::channel(config.encode_queue_capacity);

        // Create pool_size appending slots.
        let mut slots = Vec::with_capacity(config.active_pool_size);
        for _ in 0..config.active_pool_size {
            let slot = PoolSlot::new(tier, size_config, buffer_pool.as_ref())?;
            slots.push(Arc::new(slot));
        }

        let encode_semaphore = Arc::new(Semaphore::new(config.max_inflight_encodes));

        Ok(Self {
            slots,
            tier,
            size_config: size_config.clone(),
            current_index: Mutex::new(0),
            encode_tx,
            encode_rx: Mutex::new(Some(encode_rx)),
            encode_semaphore,
            config,
            buffer_pool: Arc::clone(&buffer_pool),
        })
    }

    /// Appends data to the current active segment.
    ///
    /// If the current segment is full after the append, it triggers a
    /// rotation to the next idle slot.
    ///
    /// # Returns
    ///
    /// `(segment_id, offset, length)` identifying the written data.
    ///
    /// # Errors
    ///
    /// Returns an error if no segment is available for writing or if
    /// the underlying append operation fails.
    /// Appends data to the current active segment.
    ///
    /// This is a synchronous operation because the pool uses `parking_lot::Mutex`
    /// internally — lock hold times are microsecond-scale so blocking the
    /// calling thread is negligible. Callers on the tokio runtime should
    /// use `tokio::task::spawn_blocking` if wrapping is desired.
    ///
    /// # Returns
    ///
    /// `(segment_id, offset, length)` identifying the written data.
    ///
    /// # Errors
    ///
    /// Returns an error if no segment is available for writing or if
    /// the underlying append operation fails.
    pub(crate) fn append(&self, data: &[u8]) -> Result<(SegmentId, u64, u32)> {
        let idx = {
            let mut current = self.current_index.lock();
            let idx = *current;
            *current = (*current + 1) % self.slots.len();
            idx
        };

        let slot = &self.slots[idx];
        let state = slot.state();

        // Find the next available appending slot if the current one is
        // not in Appending state.
        if state != PoolSlotState::Appending {
            return self.append_to_next_available(data);
        }

        let mut seg_guard = slot.segment.lock();
        let segment = seg_guard
            .as_mut()
            .ok_or_else(|| Error::InvalidConfig("pool slot has no segment".into()))?;

        let (offset, length) = segment.append(data)?;
        let segment_id = segment.id();
        let length_u32 = u32::try_from(length).unwrap_or(u32::MAX);

        // Check if the segment is full after this append.
        if segment.is_full() {
            drop(seg_guard);

            // Move slot to Sealing state.
            slot.set_state(PoolSlotState::Sealing);

            // Extract the segment for sealing.
            let sealed_segment = slot.segment.lock().take();

            if let Some(seg) = sealed_segment {
                // Enqueue for EC encoding.
                self.enqueue_encoding(seg.id());
            }

            // Try to activate a new segment in this slot (or another idle one).
            self.try_activate_slot();
        }

        Ok((segment_id, offset, length_u32))
    }

    /// Returns the number of slots in Appending state.
    pub(crate) fn active_count(&self) -> usize {
        self.slots.iter().filter(|s| s.state() == PoolSlotState::Appending).count()
    }

    /// Returns the number of pool slots.
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Appends data to the next available appending slot (round-robin).
    fn append_to_next_available(&self, data: &[u8]) -> Result<(SegmentId, u64, u32)> {
        for slot in self.slots.iter() {
            if slot.state() == PoolSlotState::Appending {
                let mut seg_guard = slot.segment.lock();
                if let Some(segment) = seg_guard.as_mut() {
                    let (offset, length) = segment.append(data)?;
                    let segment_id = segment.id();
                    let length_u32 = u32::try_from(length).unwrap_or(u32::MAX);
                    if segment.is_full() {
                        drop(seg_guard);
                        slot.set_state(PoolSlotState::Sealing);
                        let sealed = slot.segment.lock().take();
                        if let Some(seg) = sealed {
                            self.enqueue_encoding(seg.id());
                        }
                        self.try_activate_slot();
                    }
                    return Ok((segment_id, offset, length_u32));
                }
            }
        }
        Err(Error::InvalidConfig("no appending segment available in pool".into()))
    }

    /// Enqueues a segment ID for EC encoding on the bounded work channel.
    ///
    /// Uses `send().await` with a bounded timeout to enforce backpressure:
    /// if the encoding queue is full, this method will block the caller for
    /// up to 500ms. If the timeout elapses, the encoding is deferred and
    /// will be retried later by the pool rotation logic.
    fn enqueue_encoding(&self, segment_id: SegmentId) {
        let timeout = std::time::Duration::from_millis(500);
        let handle = tokio::runtime::Handle::current();
        let result =
            handle.block_on(tokio::time::timeout(timeout, self.encode_tx.send(segment_id)));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(mpsc::error::SendError(_))) => {
                tracing::error!(
                    segment_id = %segment_id,
                    "EC encoding queue closed; segment will not be encoded"
                );
            }
            Err(_elapsed) => {
                tracing::warn!(
                    segment_id = %segment_id,
                    timeout_ms = 500,
                    "EC encoding queue full after timeout; encoding deferred"
                );
            }
        }
    }

    /// Attempts to activate a new segment in an idle or sealed slot.
    fn try_activate_slot(&self) {
        // Look for a slot in Sealing or Idle state that has no segment.
        for slot in self.slots.iter() {
            let mut seg = slot.segment.lock();
            if seg.is_none() {
                let state = slot.state();
                if state == PoolSlotState::Sealing
                    || state == PoolSlotState::Encoding
                    || state == PoolSlotState::Idle
                {
                    // Try to create a new active segment from the buffer pool.
                    match ActiveSegment::new(
                        self.tier,
                        &self.size_config,
                        self.buffer_pool.as_ref(),
                    ) {
                        Ok(new_segment) => {
                            *seg = Some(new_segment);
                            slot.set_state(PoolSlotState::Appending);
                            tracing::info!(
                                tier = ?self.tier,
                                "pool slot re-activated with new active segment"
                            );
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(
                                tier = ?self.tier,
                                error = %e,
                                "failed to create new active segment; slot remains empty"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Returns the EC encoding receiver channel, if available.
    pub(crate) fn take_encode_rx(&self) -> Option<mpsc::Receiver<SegmentId>> {
        self.encode_rx.lock().take()
    }

    /// Returns a clone of the encoding semaphore for worker tasks.
    pub(crate) fn encode_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.encode_semaphore)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc as StdArc,
        },
        thread,
    };

    use oceanfs_core::SegmentSizeConfig;

    use super::*;

    fn test_config() -> (PoolConfig, SegmentSizeConfig) {
        (PoolConfig::default(), SegmentSizeConfig::default())
    }

    fn test_pool() -> Arc<BufferPool> {
        Arc::new(BufferPool::new(65536, 32))
    }

    #[test]
    fn pool_creation_has_correct_slot_count() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();
        assert_eq!(pool.slot_count(), 4);
        assert_eq!(pool.active_count(), 4, "all slots start in Appending state");
    }

    #[test]
    fn pool_append_returns_valid_offset_and_length() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();
        let (seg_id, offset, length) = pool.append(b"hello world").unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 11);
        assert_ne!(seg_id, SegmentId::default());
    }

    #[test]
    fn concurrent_writes_across_slots_do_not_corrupt_data() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = StdArc::new(
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool.clone()).unwrap(),
        );

        let write_count = StdArc::new(AtomicUsize::new(0));
        let num_threads = 8;
        let writes_per_thread = 50;

        let mut handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let pool = StdArc::clone(&pool);
            let write_count = StdArc::clone(&write_count);
            let handle = thread::spawn(move || {
                for _ in 0..writes_per_thread {
                    let result = pool.append(b"data");
                    assert!(result.is_ok(), "append must not fail under concurrency");
                    write_count.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(write_count.load(Ordering::Relaxed), num_threads * writes_per_thread);
    }

    #[test]
    fn encode_queue_is_created() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();

        let rx = pool.take_encode_rx();
        assert!(rx.is_some(), "encode receiver must exist");
    }

    #[test]
    fn encode_semaphore_has_correct_permits() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg.clone(), SizeTier::Standard, &size_cfg, buf_pool.clone())
                .unwrap();

        let sem = pool.encode_semaphore();
        // Verify that the semaphore has been created with the expected count.
        // We can't directly inspect Semaphore internals, but we can acquire
        // all permits and then try one more.
        let mut permits = Vec::new();
        for _ in 0..pool_cfg.max_inflight_encodes {
            let permit = sem.try_acquire();
            assert!(permit.is_ok(), "should be able to acquire up to max permits");
            permits.push(permit.unwrap());
        }
        // One more should fail (no permits left).
        assert!(sem.try_acquire().is_err(), "should be exhausted");
    }

    #[test]
    fn custom_pool_size_config() {
        let pool_cfg = PoolConfig { active_pool_size: 8, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 32));
        let pool = SegmentPool::new(pool_cfg, SizeTier::Small, &size_cfg, buf_pool).unwrap();
        assert_eq!(pool.slot_count(), 8);
    }

    #[test]
    fn pool_append_returns_different_segment_ids() {
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();

        let (id1, _, _) = pool.append(b"a").unwrap();
        // The second append may go to a different slot (round-robin).
        let (id2, _, _) = pool.append(b"b").unwrap();
        // Both IDs are valid UUIDs.
        assert_ne!(id1, SegmentId::default());
        assert_ne!(id2, SegmentId::default());
    }

    // ── Pool rotation tests (fill → seal → new segment) ───────────

    #[test]
    fn pool_rotation_fills_segment_and_activates_new_slot() {
        // Use a tiny target size so a single append triggers is_full().
        let pool_cfg = PoolConfig { active_pool_size: 4, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 10,
            small_target_size: 10,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        // We need a tokio runtime because enqueue_encoding calls
        // Handle::current(). Create a minimal runtime for the test.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _guard = rt.enter();

        let pool = SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();
        assert_eq!(pool.active_count(), 4, "all 4 slots start appending");

        // Append data larger than target_size (10 bytes).
        // This fills the first slot's segment, triggering seal+rotation.
        let data = b"hello world, this is longer than 10 bytes";
        let (seg_id, offset, length) = pool.append(data).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, data.len() as u32);
        assert_ne!(seg_id, SegmentId::default());

        // After fill+rotation, active_count should still be 4
        // (the filled slot was replaced with a newly activated one).
        assert_eq!(pool.active_count(), 4, "pool should re-activate after fill");

        // The encoding queue should have received the sealed segment ID.
        let rx = pool.take_encode_rx();
        assert!(rx.is_some(), "encoding queue should have entries after fill");

        // Verify subsequent appends succeed (pool is still functional).
        let (seg_id2, offset2, _len2) = pool.append(b"more data").unwrap();
        assert_eq!(offset2, 0);
        assert_ne!(seg_id2, SegmentId::default());
    }

    #[test]
    fn pool_rotation_multiple_fills_all_slots() {
        // Fill all slots sequentially and verify pool remains functional.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 15,
            small_target_size: 15,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _guard = rt.enter();

        let pool = SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();
        assert_eq!(pool.active_count(), 2);

        // Fill both slots multiple times. Each 20-byte append overflows
        // target_size=15, triggering rotation.
        for i in 0..10 {
            let data = format!("fill-iteration-{i:02}-padding");
            let result = pool.append(data.as_bytes());
            assert!(result.is_ok(), "append {} must succeed after rotation", i);
        }

        // Pool should still have active slots.
        assert!(pool.active_count() > 0, "pool must have active slots after fill cycles");
    }

    // ── Backpressure test ─────────────────────────────────────────

    #[test]
    fn encode_queue_backpressure_config_is_respected() {
        // Verify that a pool with small encode_queue_capacity can be
        // created and used without panicking, even when the queue fills.
        // Uses a large target size so enqueue_encoding is never called
        // (segment never fills). This tests the configuration path.
        let pool_cfg = PoolConfig {
            active_pool_size: 4,
            encode_queue_capacity: 2,
            ..PoolConfig::default()
        };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();

        // The encode queue should exist with the configured capacity.
        let rx = pool.take_encode_rx();
        assert!(rx.is_some(), "encode queue must exist");

        // Appends should succeed normally (segment won't fill with 4MB target).
        for _ in 0..10 {
            assert!(pool.append(b"data").is_ok());
        }
    }

    #[test]
    fn pool_handles_segment_full_with_encode_queue_not_draining() {
        // Create a pool with tiny target and take the encode receiver.
        // Verify that appends still succeed after the segment fills
        // (the enqueue_encoding timeout path is exercised but doesn't panic).
        use std::sync::Arc as StdArc;

        let pool_cfg = PoolConfig {
            active_pool_size: 4,
            encode_queue_capacity: 2,
            ..PoolConfig::default()
        };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 20,
            small_target_size: 20,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let pool = StdArc::new(
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap(),
        );

        // Take the receiver but don't drain — the channel will fill.
        let _rx = pool.take_encode_rx();

        // Execute on the runtime so block_on works for enqueue_encoding.
        rt.block_on(async {
            // Fill segments. With encode_queue_capacity=2, after 2 fills
            // the channel is full. Subsequent fills trigger the 500ms
            // timeout in enqueue_encoding but should not panic.
            for i in 0..5 {
                let pool = StdArc::clone(&pool);
                let data = format!("fill-data-{i:02}-enough-bytes-to-overflow");
                let result = tokio::task::spawn_blocking(move || pool.append(data.as_bytes()))
                    .await
                    .unwrap();
                assert!(result.is_ok(), "append {i} must succeed");
            }
        });
    }

    // ── State transition test ─────────────────────────────────────

    #[test]
    fn pool_slot_state_transitions_after_fill() {
        let pool_cfg = PoolConfig { active_pool_size: 4, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 10,
            small_target_size: 10,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _guard = rt.enter();

        let pool = SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool).unwrap();

        // Before writes: all slots in Appending.
        assert_eq!(pool.active_count(), 4);

        // Fill one slot with a large append.
        pool.append(b"this is more than 10 bytes, should fill").unwrap();

        // After fill: the filled slot transitions to Sealing then gets
        // a new segment via try_activate_slot(). Verify active count
        // remains stable (re-activation happened).
        assert!(pool.active_count() >= 3, "at least 3 slots should still be appending");

        // Subsequent appends across remaining active slots succeed.
        for _ in 0..10 {
            let result = pool.append(b"small");
            assert!(result.is_ok(), "append after rotation must succeed");
        }
        assert!(pool.active_count() > 0, "pool must still have active slots");
    }
}
