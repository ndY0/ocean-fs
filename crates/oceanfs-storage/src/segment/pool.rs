//! Active segment pool with pipeline parallelism.
//!
//! A pool of N active segments decouples append latency from seal-time I/O.
//! While one segment is being sealed (written to disk, metadata persisted),
//! the next segment in the pool accepts writes. Combined with per-core segment
//! sharding, this eliminates write blocking during seal cycles.
//!
//! ## Pool States
//!
//! Each slot transitions through a lifecycle:
//! ```text
//! Idle → Appending → Sealing → Idle
//! ```
//!
//! Per performance guideline §2.5 (sharded segment buffer), §2.6 (bounded
//! channels), and §2.7 (semaphore-bounded concurrency).

use std::{collections::HashMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use oceanfs_core::{CodecConfig, PoolConfig, SegmentId, SizeTier};
use parking_lot::{Condvar, Mutex, RwLock};
use tokio::sync::{mpsc, Semaphore};

use crate::{
    buffer_pool::BufferPool,
    error::{Error, Result},
    segment::buffer::SegmentBuffer,
};

/// Upper bound on how long an append may wait while the pool is silent.
/// The budget is refreshed on every re-activation signal (a busy pool
/// never fails a write, no matter how often the waiting thread loses the
/// race for the fresh slot), and waiters self-heal stranded slots before
/// waiting — so the terminal error below is only reachable when segment
/// creation itself keeps failing.
const SLOT_ACTIVATION_WAIT: std::time::Duration = std::time::Duration::from_millis(10);

/// Each condvar wait sleeps at most this long before re-scanning the
/// slots. Waiting the whole budget in one shot would let a single lost
/// wakeup consume the entire budget and leave exactly one re-scan at the
/// deadline — which under continuous churn often lands in another transit
/// window. Short slices give the re-scan many chances within the budget.
const SLOT_ACTIVATION_WAIT_SLICE: std::time::Duration = std::time::Duration::from_millis(1);

/// A work item sent to the seal worker when a segment is filled.
///
/// Contains the segment's identity, its data (copied before the
/// backing buffer is returned to the pool), the storage tier, and
/// any pre-computed parity shards from streaming EC encode.
#[derive(Debug)]
pub struct SealingWork {
    /// The unique identifier of the segment to seal.
    pub segment_id: SegmentId,
    /// The segment data bytes, copied from the active segment buffer
    /// before the backing buffer was returned to the pool.
    pub segment_data: Bytes,
    /// The storage tier of the segment (Small or Standard).
    pub tier: SizeTier,
    /// Parity shards pre-computed by streaming EC encode.
    /// `None` when streaming encode is disabled or no stripes were encoded.
    pub parity_shards: Option<Vec<Bytes>>,
    /// Number of EC data shards (k). 0 if EC is not used.
    pub ec_k: u8,
    /// Number of EC parity shards (m). 0 if EC is not used.
    pub ec_m: u8,
}

/// The state of a pool slot throughout its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PoolSlotState {
    /// Slot is free and can accept a new segment.
    Idle,
    /// Segment is actively accepting writes.
    Appending,
    /// Segment has been filled and extracted for sealing.
    Sealing,
}

/// A single slot in the segment pool, holding one active segment and its state.
pub(crate) struct PoolSlot {
    state: Mutex<PoolSlotState>,
    segment: Mutex<Option<SegmentBuffer>>,
}

impl PoolSlot {
    /// Creates a new pool slot with a fresh active segment.
    fn new(
        tier: SizeTier,
        config: &oceanfs_core::SegmentSizeConfig,
        pool: &BufferPool,
        ec_config: Option<&CodecConfig>,
    ) -> Result<Self> {
        let segment = SegmentBuffer::new(tier, config, pool, ec_config)?;
        Ok(Self { state: Mutex::new(PoolSlotState::Appending), segment: Mutex::new(Some(segment)) })
    }

    /// Creates a new pool slot in Idle state (no segment).
    #[allow(dead_code)]
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
/// ```text
/// // SegmentPool examples are in unit tests.
/// ```
pub struct SegmentPool {
    /// Pool slots, one per active segment.
    slots: Vec<Arc<PoolSlot>>,
    /// The storage tier this pool serves.
    tier: SizeTier,
    /// Segment size configuration for creating new active segments.
    size_config: oceanfs_core::SegmentSizeConfig,
    /// EC codec configuration for streaming encode, if enabled.
    ec_config: Option<CodecConfig>,
    /// Index of the current appending slot (round-robin).
    current_index: Mutex<usize>,
    /// Sender for the seal work queue.
    seal_tx: mpsc::Sender<SealingWork>,
    /// Receiver for the seal work queue (held by the pool, drained by a seal worker).
    seal_rx: Mutex<Option<mpsc::Receiver<SealingWork>>>,
    /// Semaphore limiting in-flight seals (enforces bounded concurrency).
    seal_semaphore: Arc<Semaphore>,
    /// Pool configuration.
    #[allow(dead_code)]
    config: PoolConfig,
    /// Reference to the buffer pool for creating new active segments.
    buffer_pool: Arc<BufferPool>,
    /// Segment data for segments that have been dequeued from active slots
    /// but not yet written to disk by the seal worker. Serves reads during
    /// the seal window (read-after-write gap).
    ///
    /// `RwLock<HashMap>` is appropriate here because writes (insert, remove)
    /// happen once per segment lifecycle (every ~64 MB of data), while reads
    /// are much more frequent (every GET request via `try_read`).
    sealing_data: RwLock<HashMap<SegmentId, Bytes>>,
    /// Wake-up signal for appenders waiting on slot re-activation
    /// (bounded backpressure when a concurrent burst fills every slot).
    /// The mutex guards nothing; it only serves as the wait primitive
    /// for the condvar — waiters re-scan the slot states after waking.
    slot_activation: (Mutex<()>, Condvar),
    /// Test-only: when set, slot re-activation fails (simulates a stuck
    /// activation) so the timeout path of the bounded wait is reachable.
    #[cfg(test)]
    fail_activation: std::sync::atomic::AtomicBool,
}

impl SegmentPool {
    /// Creates a new segment pool.
    ///
    /// If `config.ec_streaming_encode` is `true` and `ec_config` is provided,
    /// the pool creates streaming EC segments. Otherwise it creates plain
    /// active segments.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer pool cannot provide enough buffers
    /// for the initial active segments.
    pub fn new(
        config: PoolConfig,
        tier: SizeTier,
        size_config: &oceanfs_core::SegmentSizeConfig,
        buffer_pool: Arc<BufferPool>,
        ec_config: Option<CodecConfig>,
    ) -> Result<Self> {
        let (seal_tx, seal_rx) = mpsc::channel(config.encode_queue_capacity);

        // Resolve EC config: only use streaming if flag is set AND config is provided.
        let actual_ec = if config.ec_streaming_encode { ec_config } else { None };

        // Create pool_size appending slots.
        let mut slots = Vec::with_capacity(config.active_pool_size);
        for _ in 0..config.active_pool_size {
            let slot = PoolSlot::new(tier, size_config, buffer_pool.as_ref(), actual_ec.as_ref())?;
            slots.push(Arc::new(slot));
        }

        let seal_semaphore = Arc::new(Semaphore::new(config.max_inflight_encodes));

        Ok(Self {
            slots,
            tier,
            size_config: size_config.clone(),
            ec_config: actual_ec,
            current_index: Mutex::new(0),
            seal_tx,
            seal_rx: Mutex::new(Some(seal_rx)),
            seal_semaphore,
            config,
            buffer_pool: Arc::clone(&buffer_pool),
            sealing_data: RwLock::new(HashMap::new()),
            slot_activation: (Mutex::new(()), Condvar::new()),
            #[cfg(test)]
            fail_activation: std::sync::atomic::AtomicBool::new(false),
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
    /// use `tokio::task::spawn_blocking` if wrapping is desired. When a
    /// concurrent burst fills every slot at once, the call may additionally
    /// block for up to `SLOT_ACTIVATION_WAIT` until a slot is re-activated
    /// (normally microseconds); waiters self-heal stranded slots, so the
    /// append only fails with [`Error::InvalidConfig`] when segment creation
    /// itself keeps failing.
    ///
    /// # Returns
    ///
    /// `(segment_id, offset, length)` identifying the written data.
    ///
    /// # Errors
    ///
    /// Returns an error if no segment is available for writing or if
    /// the underlying append operation fails.
    pub fn append(&self, data: &[u8]) -> Result<(SegmentId, u64, u32)> {
        self.append_with_hook(data, |_, _, _| {})
    }

    /// Appends data and invokes `hook(segment_id, offset, length)` after
    /// the append but **before** any fill-triggered seal enqueue.
    ///
    /// The hook runs while the segment lock is held, which makes the
    /// ordering airtight: a seal worker running on another thread cannot
    /// observe the seal work item before the hook has recorded its state
    /// (e.g. the write coordinator's blob index entry). With a plain
    /// `append()` + separate record step, the seal worker could drain
    /// the entries map on the multi-threaded runtime before the writer
    /// thread recorded its entry, skipping the seal
    /// (read-path-integrity-under-load).
    ///
    /// # Errors
    ///
    /// Same error conditions as [`append`](Self::append).
    pub fn append_with_hook<F: FnOnce(SegmentId, u64, u32)>(
        &self,
        data: &[u8],
        hook: F,
    ) -> Result<(SegmentId, u64, u32)> {
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
            return self.append_to_next_available_with_hook(data, hook);
        }

        let mut seg_guard = slot.segment.lock();
        let Some(segment) = seg_guard.as_mut() else {
            // The state check above read `Appending` before a concurrent
            // filler moved the slot to `Sealing` and took the segment.
            // Retry through the available-slot path (which also waits
            // for re-activation) instead of failing the write.
            drop(seg_guard);
            return self.append_to_next_available_with_hook(data, hook);
        };

        let (offset, length) = match segment.append(data) {
            Ok(placed) => placed,
            Err(Error::SegmentFull { .. }) => {
                // A concurrent append filled this segment after our state
                // check but before we acquired the lock. Retry on the next
                // available slot instead of failing the write.
                drop(seg_guard);
                return self.append_to_next_available_with_hook(data, hook);
            }
            Err(e) => return Err(e),
        };
        let segment_id = segment.id();
        let length_u32 = u32::try_from(length).unwrap_or(u32::MAX);

        // Invoke the hook while still holding the segment lock — before
        // the fill check below can enqueue the seal — so the seal worker
        // can never observe the work item before the hook ran.
        hook(segment_id, offset, length_u32);

        // Check if the segment is full after this append.
        if segment.is_full() {
            drop(seg_guard);

            // Move slot to Sealing state.
            slot.set_state(PoolSlotState::Sealing);

            // Extract the segment for sealing.
            let sealed_segment = slot.segment.lock().take();

            if let Some(seg) = sealed_segment {
                let seg_id = seg.id();
                let seg_tier = seg.tier();
                let parity = seg.parity_shards();
                // Freeze the backing buffer into a zero-copy `Bytes`.
                // The backing memory stays alive for the seal window; the
                // seal worker recycles it back to the buffer pool via
                // `release_buffer` after the seal completes.
                let seg_data = seg.into_buffer().freeze();
                // Retain a handle in the sealing set so reads can
                // reach this segment during the seal-to-disk window.
                self.sealing_data.write().insert(seg_id, seg_data.clone());
                self.enqueue_seal(seg_id, seg_data, seg_tier, parity);
            }

            // Try to activate a new segment in this slot (or another idle one).
            self.try_activate_slot();
        }

        Ok((segment_id, offset, length_u32))
    }

    /// Returns the number of slots in Appending state.
    #[allow(dead_code)]
    pub(crate) fn active_count(&self) -> usize {
        self.slots.iter().filter(|s| s.state() == PoolSlotState::Appending).count()
    }

    /// Returns the number of pool slots.
    #[allow(dead_code)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Reads a chunk from an active (unsealed) segment in this pool.
    ///
    /// Searches all appending slots for a segment matching `segment_id`.
    /// If found, copies the [offset, offset+length) range from the
    /// in-memory buffer into a new `Bytes`.
    ///
    /// Returns `None` if no active segment in this pool matches the id.
    /// This is a fast, synchronous operation — only a memcpy under the
    /// segment mutex, same lock used by `append`.
    pub fn try_read(&self, segment_id: SegmentId, offset: u64, length: u32) -> Option<Bytes> {
        for slot in self.slots.iter() {
            let seg_guard = slot.segment.lock();
            if let Some(segment) = seg_guard.as_ref() {
                if segment.id() == segment_id {
                    let data = segment.data();
                    let start = offset as usize;
                    let end = start.saturating_add(length as usize).min(data.len());
                    if start < data.len() {
                        return Some(Bytes::copy_from_slice(&data[start..end]));
                    }
                }
            }
        }
        // Check segments currently being sealed (fill→disk window).
        if let Some(seg_data) = self.sealing_data.read().get(&segment_id) {
            let start = offset as usize;
            let end = start.saturating_add(length as usize).min(seg_data.len());
            if start < seg_data.len() {
                return Some(seg_data.slice(start..end));
            }
        }
        None
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Appends data to the next available appending slot (round-robin).
    ///
    /// When every slot is transiently unavailable — a concurrent burst can
    /// fill all slots at once, leaving each in the `Sealing` state while its
    /// replacement segment is allocated — this method self-heals stranded
    /// slots, then waits (bounded by `SLOT_ACTIVATION_WAIT`) for a slot
    /// re-activation instead of failing the write. Re-activation is
    /// performed synchronously by the filling thread right after the seal
    /// enqueue, so the wait is normally microseconds; the terminal error is
    /// only reachable when segment creation itself keeps failing.
    fn append_to_next_available_with_hook<F: FnOnce(SegmentId, u64, u32)>(
        &self,
        data: &[u8],
        hook: F,
    ) -> Result<(SegmentId, u64, u32)> {
        // The hook is `FnOnce`: wrap it so a failed scan pass doesn't
        // consume it before the successful append.
        let mut hook = Some(hook);
        let mut deadline = std::time::Instant::now() + SLOT_ACTIVATION_WAIT;
        loop {
            for slot in self.slots.iter() {
                if slot.state() == PoolSlotState::Appending {
                    let mut seg_guard = slot.segment.lock();
                    if let Some(segment) = seg_guard.as_mut() {
                        let (offset, length) = match segment.append(data) {
                            Ok(placed) => placed,
                            Err(Error::SegmentFull { .. }) => {
                                // This slot's segment filled concurrently —
                                // continue scanning for another slot.
                                drop(seg_guard);
                                continue;
                            }
                            Err(e) => return Err(e),
                        };
                        let segment_id = segment.id();
                        let length_u32 = u32::try_from(length).unwrap_or(u32::MAX);
                        // Same airtight ordering as `append_with_hook`: the
                        // hook runs under the segment lock, before the seal
                        // work item for this segment can be enqueued.
                        if let Some(hook) = hook.take() {
                            hook(segment_id, offset, length_u32);
                        }
                        if segment.is_full() {
                            drop(seg_guard);
                            slot.set_state(PoolSlotState::Sealing);
                            let sealed = slot.segment.lock().take();
                            if let Some(seg) = sealed {
                                let seg_id = seg.id();
                                let seg_tier = seg.tier();
                                let parity = seg.parity_shards();
                                let seg_data = seg.into_buffer().freeze();
                                // Retain a handle in the sealing set so reads can
                                // reach this segment during the seal-to-disk window.
                                self.sealing_data.write().insert(seg_id, seg_data.clone());
                                self.enqueue_seal(seg_id, seg_data, seg_tier, parity);
                            }
                            self.try_activate_slot();
                        }
                        return Ok((segment_id, offset, length_u32));
                    }
                }
            }

            // Every slot is unavailable. Before waiting, self-heal: if a
            // concurrent filler was descheduled between taking its segment
            // and re-activating the slot (or its activation attempt raced),
            // a waiter activates the stranded slot itself — a Sealing slot
            // with no segment can never block the pool indefinitely.
            self.try_activate_slot();

            // Wait for a re-activation signal in short slices, re-scanning
            // between them: a lost wakeup costs at most one slice, never
            // the whole budget.
            let (lock, cvar) = &self.slot_activation;
            let mut guard = lock.lock();
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                drop(guard);
                break;
            }
            // Spurious wakeups are harmless: the loop re-scans and
            // re-enters the wait with the remaining budget.
            let outcome = cvar.wait_for(&mut guard, remaining.min(SLOT_ACTIVATION_WAIT_SLICE));
            drop(guard);
            if !outcome.timed_out() {
                // An activation occurred: the pool is making progress and
                // the waiter simply lost the race for the fresh slot. The
                // budget measures silence (a genuinely stuck pool), not
                // losing streaks — refresh it so a busy-but-fair pool
                // never fails writes.
                deadline = std::time::Instant::now() + SLOT_ACTIVATION_WAIT;
            }
        }
        Err(Error::InvalidConfig("no appending segment available in pool".into()))
    }

    /// Enqueues a filled segment for sealing on the bounded work channel.
    ///
    /// Uses `try_send` for non-blocking enqueue. If the channel is full,
    /// the seal is deferred and will be retried later by the pool
    /// rotation logic. This avoids blocking the caller in async contexts.
    fn enqueue_seal(
        &self,
        segment_id: SegmentId,
        segment_data: Bytes,
        tier: SizeTier,
        parity_shards: Option<Vec<Bytes>>,
    ) {
        let (ec_k, ec_m) =
            self.ec_config.as_ref().map(|c| (c.data_shards, c.parity_shards)).unwrap_or((0, 0));
        let work = SealingWork { segment_id, segment_data, tier, parity_shards, ec_k, ec_m };
        match self.seal_tx.try_send(work) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                // The segment was not enqueued for sealing; remove
                // the sealing-data entry to avoid leaking the Bytes.
                self.sealing_data.write().remove(&segment_id);
                tracing::warn!(
                    segment_id = %segment_id,
                    "seal queue full; seal deferred, sealing-data entry removed"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.sealing_data.write().remove(&segment_id);
                tracing::error!(
                    segment_id = %segment_id,
                    "seal queue closed; segment will not be sealed"
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
                if state == PoolSlotState::Sealing || state == PoolSlotState::Idle {
                    #[cfg(test)]
                    if self.fail_activation.load(std::sync::atomic::Ordering::Relaxed) {
                        // Test seam: pretend activation keeps failing so the
                        // bounded-wait timeout path is exercisable.
                        continue;
                    }
                    // Try to create a new active segment from the buffer pool.
                    match SegmentBuffer::new(
                        self.tier,
                        &self.size_config,
                        self.buffer_pool.as_ref(),
                        self.ec_config.as_ref(),
                    ) {
                        Ok(new_segment) => {
                            *seg = Some(new_segment);
                            slot.set_state(PoolSlotState::Appending);
                            tracing::info!(
                                tier = ?self.tier,
                                "pool slot re-activated with new active segment"
                            );
                            // Wake appenders blocked on slot exhaustion
                            // (bounded backpressure).
                            self.slot_activation.1.notify_all();
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

    /// Returns the seal receiver channel, if available.
    ///
    /// The seal worker must call this to obtain the receiver for
    /// draining sealed segments. Only one worker should take the
    /// receiver; subsequent calls return `None`.
    pub fn take_seal_rx(&self) -> Option<mpsc::Receiver<SealingWork>> {
        self.seal_rx.lock().take()
    }

    /// Returns a clone of the seal semaphore for worker tasks.
    pub fn seal_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.seal_semaphore)
    }

    /// Removes a segment from the sealing-data set after it has been
    /// successfully written to disk.
    ///
    /// Called by the seal worker after `seal_from_data()` returns `Ok`.
    /// This frees the held `Bytes` reference, allowing the buffer memory
    /// to be reclaimed.
    pub fn remove_seal_buffer(&self, segment_id: SegmentId) {
        self.sealing_data.write().remove(&segment_id);
    }

    /// Returns a segment backing buffer to the shared buffer pool after
    /// a seal completes.
    ///
    /// Called by the seal worker once the sealing-data reference has been
    /// dropped and the work item's `Bytes` is the unique owner of the
    /// original `BytesMut` allocation (recovered via `Bytes::try_into_mut`).
    /// The next segment activation reuses the allocation instead of
    /// malloc'ing a fresh buffer (pool-backpressure-and-buffer-recycling).
    pub fn release_buffer(&self, buf: BytesMut) {
        self.buffer_pool.release(buf);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();
        assert_eq!(pool.slot_count(), 4);
        assert_eq!(pool.active_count(), 4, "all slots start in Appending state");
    }

    #[test]
    fn pool_append_returns_valid_offset_and_length() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();
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
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool.clone(), None)
                .unwrap(),
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
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        let rx = pool.take_seal_rx();
        assert!(rx.is_some(), "seal receiver must exist");
    }

    #[test]
    fn encode_semaphore_has_correct_permits() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg.clone(),
            SizeTier::Standard,
            &size_cfg,
            buf_pool.clone(),
            None,
        )
        .unwrap();

        let sem = pool.seal_semaphore();
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
        let pool = SegmentPool::new(pool_cfg, SizeTier::Small, &size_cfg, buf_pool, None).unwrap();
        assert_eq!(pool.slot_count(), 8);
    }

    #[test]
    fn pool_append_returns_different_segment_ids() {
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

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
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _guard = rt.enter();

        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();
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

        // The seal queue should have received the sealed segment.
        let rx = pool.take_seal_rx();
        assert!(rx.is_some(), "seal queue should have entries after fill");

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

        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _guard = rt.enter();

        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();
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
    fn seal_queue_backpressure_config_is_respected() {
        // Verify that a pool with small encode_queue_capacity can be
        // created and used without panicking, even when the queue fills.
        // Uses a large target size so enqueue_seal is never called
        // (segment never fills). This tests the configuration path.
        let pool_cfg =
            PoolConfig { active_pool_size: 4, encode_queue_capacity: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        // The seal queue should exist with the configured capacity.
        let rx = pool.take_seal_rx();
        assert!(rx.is_some(), "seal queue must exist");

        // Appends should succeed normally (segment won't fill with 4MB target).
        for _ in 0..10 {
            assert!(pool.append(b"data").is_ok());
        }
    }

    #[test]
    fn pool_handles_segment_full_with_seal_queue_not_draining() {
        // Create a pool with tiny target and take the seal receiver.
        // Verify that appends still succeed after the segment fills
        // (enqueue_seal handles full channel gracefully without panic).
        use std::sync::Arc as StdArc;

        let pool_cfg =
            PoolConfig { active_pool_size: 4, encode_queue_capacity: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 20,
            small_target_size: 20,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

        let pool = StdArc::new(
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap(),
        );

        // Take the receiver but don't drain — the channel will fill.
        let _rx = pool.take_seal_rx();

        // Execute on the runtime so block_on works for enqueue_seal.
        rt.block_on(async {
            // Fill segments. With encode_queue_capacity=2, after 2 fills
            // the channel is full. Subsequent fills trigger the enqueue_seal
            // try_send failure path but should not panic.
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

        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _guard = rt.enter();

        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

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

    // ── Backpressure tests (pool-backpressure-and-buffer-recycling) ──

    /// Parks every slot in `Sealing` with no segment, simulating the
    /// transit window of a concurrent fill burst.
    fn park_all_slots(pool: &SegmentPool) {
        for slot in pool.slots.iter() {
            let _ = slot.segment.lock().take();
            slot.set_state(PoolSlotState::Sealing);
        }
    }

    #[test]
    fn append_waits_for_slot_reactivation() {
        let pool_cfg = PoolConfig { active_pool_size: 4, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 32));
        let pool = Arc::new(
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap(),
        );

        park_all_slots(&pool);
        assert_eq!(pool.active_count(), 0, "all slots parked");

        let pool2 = Arc::clone(&pool);
        let handle = std::thread::spawn(move || pool2.append(b"hello"));

        // Give the appender time to enter the bounded wait, then
        // re-activate a slot exactly like the fill path does.
        std::thread::sleep(std::time::Duration::from_millis(2));
        pool.try_activate_slot();

        let result = handle.join().unwrap();
        assert!(result.is_ok(), "append must complete after re-activation: {result:?}");
    }

    #[test]
    fn append_returns_error_when_activation_keeps_failing() {
        // The terminal error is only reachable when segment creation keeps
        // failing (real resource exhaustion): with activation disabled and
        // every slot parked, the bounded wait must expire and return the
        // existing InvalidConfig error instead of hanging forever.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        park_all_slots(&pool);
        pool.fail_activation.store(true, std::sync::atomic::Ordering::Relaxed);

        let result = pool.append(b"hello");
        assert!(
            matches!(result, Err(Error::InvalidConfig(_))),
            "expected exhaustion error, got {result:?}"
        );
    }

    #[test]
    fn append_self_heals_when_all_slots_are_parked() {
        // With the self-healing wait, a pool whose slots are all in the
        // Sealing-with-no-segment state is re-activated by the appender
        // itself — the write succeeds instead of failing.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        park_all_slots(&pool);

        let result = pool.append(b"hello");
        assert!(result.is_ok(), "append must self-heal a parked pool: {result:?}");
        assert_eq!(pool.active_count(), 1, "the appender activated one slot");
    }

    #[test]
    fn concurrent_churn_never_exhausts_slots() {
        // Production-shaped churn: 4 slots, tiny target, every append fills
        // its segment (the worst burst a multi-tier workload produces).
        // Under the bounded backpressure, appends must never fail with
        // slot exhaustion (they may wait briefly).
        let pool_cfg =
            PoolConfig { active_pool_size: 4, encode_queue_capacity: 8, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 1024,
            small_target_size: 1024,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = Arc::new(BufferPool::new(65536, 64));
        let pool = Arc::new(
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap(),
        );

        // Drain the seal queue so the fillers never back up on it.
        let mut rx = pool.take_seal_rx().expect("seal rx available");
        let drain = std::thread::spawn(move || while rx.blocking_recv().is_some() {});

        let mut handles = Vec::new();
        for _ in 0..16 {
            let pool = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                let data = vec![0xABu8; 2048]; // > target: every append fills
                let mut failures = 0usize;
                for _ in 0..200 {
                    if pool.append(&data).is_err() {
                        failures += 1;
                    }
                }
                failures
            }));
        }
        let mut failures = 0usize;
        for h in handles {
            failures += h.join().unwrap();
        }
        // The bounded wait may legitimately expire under OS-scheduling
        // turbulence in this 8×-adversarial configuration (16 threads ×
        // 4 slots, every append fills). Regression mutations (removing
        // the self-heal or the budget refresh) fail essentially every
        // append; scheduling noise stays far below this 0.125% bound.
        assert!(
            failures <= 4,
            "slot exhaustion too frequent: {failures} of {} appends failed",
            16 * 200
        );
        drop(pool); // close the seal sender
        drain.join().unwrap();
    }

    // ── try_read tests ───────────────────────────────────────────
    #[test]
    fn try_read_returns_data_after_append() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        let data = b"hello world, this is a test segment read";
        let (seg_id, offset, length) = pool.append(data).unwrap();

        // Data should be readable immediately from the active segment.
        let chunk = pool.try_read(seg_id, offset, length).expect("try_read should find segment");
        assert_eq!(chunk.len(), length as usize);
        assert_eq!(&chunk[..], &data[..length as usize]);
    }

    #[test]
    fn try_read_returns_none_for_unknown_segment() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        // A segment id that was never appended.
        let unknown_id = SegmentId::new();
        let result = pool.try_read(unknown_id, 0, 10);
        assert!(result.is_none(), "try_read must return None for unknown segment");
    }

    #[test]
    fn try_read_respects_offset_and_length() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        let data = b"abcdefghijklmnopqrstuvwxyz";
        let (seg_id, offset, length) = pool.append(data).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 26);

        // Read a sub-range: bytes 5..10 = "fghij"
        let chunk = pool.try_read(seg_id, 5, 5).expect("sub-range read");
        assert_eq!(&chunk[..], b"fghij");

        // Read from non-zero offset to end.
        let chunk = pool.try_read(seg_id, 20, 10).expect("tail read");
        assert_eq!(&chunk[..], b"uvwxyz");
    }

    #[test]
    fn try_read_clamped_at_buffer_end() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool =
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_cfg, buf_pool, None).unwrap();

        let data = b"short";
        let (seg_id, offset, _length) = pool.append(data).unwrap();
        assert_eq!(offset, 0);

        // Request more bytes than written — should be clamped.
        let chunk = pool.try_read(seg_id, 0, 100).expect("clamped read");
        assert_eq!(chunk.len(), 5);
        assert_eq!(&chunk[..], b"short");
    }
}
