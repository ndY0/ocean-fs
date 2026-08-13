//! Recyclable buffer pool for segment append operations.
//!
//! Pre-allocates a pool of `BytesMut` buffers and recycles them between
//! segment lifecycles. This avoids repeated allocation pressure during
//! high-throughput write workloads.
//!
//! Per performance guideline §1.2: arena/buffer pool for segment append
//! buffers.

use bytes::BytesMut;
use oceanfs_core::{Gauge, LabelSet, MetricRegistrar};
use parking_lot::Mutex;

/// Buffers at or below this capacity belong to the small size class
/// (small-tier segments); larger buffers are standard/multi segment
/// buffers in the large class. The threshold comfortably covers the
/// small tier's segment sizes (64 KB target, up to 256 KB blobs).
const SMALL_CLASS_THRESHOLD: usize = 256 * 1024;

/// A pool of reusable `BytesMut` buffers for active segment writing.
///
/// Buffers are acquired from the pool when a new active segment is
/// created, and released back when the segment is sealed. This
/// amortizes allocation cost to pool initialization.
///
/// The pool keeps two size classes: small buffers (≤
/// `SMALL_CLASS_THRESHOLD`, pre-allocated eagerly for the small tier)
/// and large buffers (standard/multi segments, e.g. 4 MiB — allocated
/// lazily and recycled after sealing). Each class is bounded by a byte
/// budget rather than a buffer count, so recycling a handful of large
/// segment buffers cannot balloon retained memory.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::BufferPool;
///
/// let pool = BufferPool::new(65536, 4);
/// let mut buf = pool.acquire();
/// buf.extend_from_slice(b"hello");
/// pool.release(buf);
/// ```
pub struct BufferPool {
    /// Small buffers (≤ [`SMALL_CLASS_THRESHOLD`]); eagerly pre-allocated.
    small: SizeClass,
    /// Large buffers (standard/multi segments); lazily allocated and
    /// recycled via [`release`](BufferPool::release) after sealing.
    large: SizeClass,
    /// Size of each pre-allocated small buffer in bytes.
    chunk_size: usize,
    /// Maximum number of small buffers pre-allocated at startup.
    max_buffers: usize,
    /// Total number of buffers created at initialization.
    total_created: usize,
}

/// One size class of recycled buffers, bounded in retained bytes.
struct SizeClass {
    /// Available buffers and the total capacity they retain.
    inner: Mutex<SizeClassInner>,
    /// Byte budget: `release` drops buffers beyond this total.
    max_bytes: usize,
}

/// Mutex-guarded state of a size class.
struct SizeClassInner {
    /// Available buffers ready for acquisition.
    free: Vec<BytesMut>,
    /// Total capacity retained in `free`.
    retained_bytes: usize,
}

impl SizeClass {
    /// Creates an empty class with the given byte budget.
    fn new(max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(SizeClassInner { free: Vec::new(), retained_bytes: 0 }),
            max_bytes,
        }
    }

    /// Pops a buffer from the free list, adjusting the retained total.
    fn pop(&self) -> Option<BytesMut> {
        let mut inner = self.inner.lock();
        let buf = inner.free.pop()?;
        inner.retained_bytes = inner.retained_bytes.saturating_sub(buf.capacity());
        Some(buf)
    }

    /// Pushes a buffer into the free list if the byte budget allows.
    ///
    /// Returns `true` if the buffer was retained.
    fn push(&self, buf: BytesMut) -> bool {
        let mut inner = self.inner.lock();
        if inner.retained_bytes.saturating_add(buf.capacity()) <= self.max_bytes {
            inner.retained_bytes += buf.capacity();
            inner.free.push(buf);
            true
        } else {
            false
        }
    }

    /// Returns the number of free buffers in this class.
    fn len(&self) -> usize {
        self.inner.lock().free.len()
    }
}

impl BufferPool {
    /// Creates a new buffer pool.
    ///
    /// `chunk_size` is the size of each small-class buffer in bytes;
    /// `max_buffers` is the number of small buffers pre-allocated
    /// eagerly. Each size class is bounded by a byte budget of
    /// `chunk_size * max_buffers`, so at most
    /// `chunk_size * max_buffers / segment_size` large segment buffers
    /// are retained after recycling.
    pub fn new(chunk_size: usize, max_buffers: usize) -> Self {
        let budget = chunk_size.saturating_mul(max_buffers);
        let small = SizeClass::new(budget);
        // Pre-allocate all small buffers eagerly.
        for _ in 0..max_buffers {
            small.push(BytesMut::with_capacity(chunk_size));
        }
        let large = SizeClass::new(budget);
        Self { small, large, chunk_size, max_buffers, total_created: max_buffers }
    }

    /// Acquires a buffer from the small size class.
    ///
    /// Returns a pre-allocated buffer from the free list if available;
    /// otherwise allocates a new buffer on demand. This fallback
    /// allocation allows the pool to tolerate temporary demand spikes
    /// and zero-copy freeze paths without blocking.
    pub fn acquire(&self) -> BytesMut {
        self.small.pop().unwrap_or_else(|| BytesMut::with_capacity(self.chunk_size))
    }

    /// Acquires a buffer with at least `capacity` bytes.
    ///
    /// Prefer this over [`acquire()`](Self::acquire) when the required size is known
    /// (e.g., segment buffers whose target size exceeds the pool's
    /// chunk size). Requests larger than `SMALL_CLASS_THRESHOLD` come
    /// from the large size class — a recycled 4 MiB segment buffer is
    /// reused without any reallocation. If the acquired buffer is
    /// smaller than `capacity`, it is transparently resized.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::BufferPool;
    ///
    /// let pool = BufferPool::new(65536, 4);
    /// // Request 4 MB — the pool's 64 KB chunks are too small, so
    /// // the buffer is transparently grown.
    /// let buf = pool.acquire_sized(4_194_304);
    /// assert!(buf.capacity() >= 4_194_304);
    /// ```
    pub fn acquire_sized(&self, capacity: usize) -> BytesMut {
        let (class, fresh_capacity) = if capacity <= SMALL_CLASS_THRESHOLD {
            (&self.small, self.chunk_size)
        } else {
            (&self.large, capacity)
        };
        let mut buf = class.pop().unwrap_or_else(|| BytesMut::with_capacity(fresh_capacity));
        if buf.capacity() < capacity {
            buf.reserve(capacity);
        }
        buf
    }

    /// Releases a buffer back to the pool for reuse.
    ///
    /// The buffer returns to the size class matching its capacity. If
    /// the class already retains its byte budget, the buffer is dropped
    /// instead — recycled large segment buffers can never balloon
    /// retained memory beyond the budget.
    pub fn release(&self, mut buf: BytesMut) {
        buf.clear();
        let capacity = buf.capacity();
        let class = if capacity <= SMALL_CLASS_THRESHOLD { &self.small } else { &self.large };
        // A false return drops the excess buffer, keeping retained
        // memory bounded by the class budget.
        let _ = class.push(buf);
    }

    /// Returns the number of free buffers currently available (both
    /// size classes).
    pub fn free_count(&self) -> usize {
        self.small.len() + self.large.len()
    }

    /// Returns the chunk size for buffers in this pool.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Returns the maximum number of buffers in the pool.
    pub fn max_buffers(&self) -> usize {
        self.max_buffers
    }

    /// Returns the total number of buffers ever created by this pool.
    pub fn total_created(&self) -> usize {
        self.total_created
    }

    /// Registers buffer pool gauges with a metrics registrar.
    ///
    /// Gauges reflect approximate live state (polled periodically).
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        let available = Gauge::new(
            "buffer_pool_buffers_available".into(),
            "Free buffers in the segment buffer pool".into(),
            LabelSet::empty(),
        );
        let allocated = Gauge::new(
            "buffer_pool_bytes_allocated".into(),
            "Total bytes allocated in the buffer pool".into(),
            LabelSet::empty(),
        );
        // Set initial values.
        available.set(self.free_count() as u64);
        allocated.set((self.total_created * self.chunk_size) as u64);
        registrar.register_gauge(available);
        registrar.register_gauge(allocated);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn acquire_returns_pre_allocated_buffer() {
        let pool = BufferPool::new(1024, 2);
        let buf = pool.acquire();
        assert_eq!(buf.capacity(), 1024);
        assert!(buf.is_empty());
    }

    #[test]
    fn acquire_allocates_on_demand_when_pool_empty() {
        let pool = BufferPool::new(1024, 2);
        let _b1 = pool.acquire();
        let _b2 = pool.acquire();
        // Pool is exhausted but acquire allocates a fresh buffer.
        let b3 = pool.acquire();
        assert_eq!(b3.capacity(), 1024);
    }

    #[test]
    fn release_makes_buffer_available_again() {
        let pool = BufferPool::new(1024, 2);
        let b1 = pool.acquire();
        assert_eq!(pool.free_count(), 1);
        pool.release(b1);
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn release_clears_buffer() {
        let pool = BufferPool::new(1024, 1);
        let mut buf = pool.acquire();
        buf.extend_from_slice(&[1, 2, 3]);
        pool.release(buf);
        let reused = pool.acquire();
        assert!(reused.is_empty());
    }

    #[test]
    fn release_beyond_max_drops_buffer() {
        let pool = BufferPool::new(1024, 1);
        let b1 = pool.acquire();
        // Acquire another (the pool creates one on demand? No — it was
        // pre-allocated. Let's take the only slot, release it, acquire
        // it again, then release a second buffer.
        pool.release(b1);
        let _b2 = pool.acquire(); // pool is now empty
                                  // Create an external buffer and release it
        let extra = BytesMut::with_capacity(1024);
        pool.release(extra); // pool had 0 free, now 1
        assert_eq!(pool.free_count(), 1);
        // Release another extra — pool should not grow beyond max
        let extra2 = BytesMut::with_capacity(1024);
        pool.release(extra2); // pool already has 1, max=1 → drops extra2
        assert_eq!(pool.free_count(), 1);
    }

    #[test]
    fn free_count_is_accurate() {
        let pool = BufferPool::new(1024, 5);
        assert_eq!(pool.free_count(), 5);
        let _b = pool.acquire();
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn chunk_size_and_max_buffers_match_config() {
        let pool = BufferPool::new(65536, 8);
        assert_eq!(pool.chunk_size(), 65536);
        assert_eq!(pool.max_buffers(), 8);
    }

    #[test]
    fn total_created_matches_max_on_init() {
        let pool = BufferPool::new(4096, 3);
        assert_eq!(pool.total_created(), 3);
    }

    // ── Size-class recycling tests (pool-backpressure-and-buffer-recycling) ──

    #[test]
    fn large_buffer_released_and_reacquired_without_resize() {
        // 64 KiB × 64 = 4 MiB budget: exactly one recycled 4 MiB buffer fits.
        let pool = BufferPool::new(65536, 64);
        let mut buf = pool.acquire_sized(4 * 1024 * 1024);
        buf.extend_from_slice(&[0xABu8; 16]);
        let capacity = buf.capacity();
        pool.release(buf);

        assert_eq!(pool.free_count(), 65, "64 pre-allocated small + 1 recycled large");
        let reused = pool.acquire_sized(4 * 1024 * 1024);
        assert!(reused.is_empty(), "released buffers are cleared");
        assert!(reused.capacity() >= capacity, "recycled buffer must be reused without shrinking");
        assert!(reused.capacity() >= 4 * 1024 * 1024);
    }

    #[test]
    fn large_class_byte_budget_bounds_retained_memory() {
        // chunk 64 KB × max 2 = 128 KB budget per class: 1 MiB buffers
        // must never be retained.
        let pool = BufferPool::new(65536, 2);
        for _ in 0..8 {
            let buf = pool.acquire_sized(1024 * 1024);
            pool.release(buf);
        }
        // Only the 2 pre-allocated small buffers may be free.
        assert_eq!(pool.free_count(), 2, "large buffers beyond the byte budget must be dropped");
    }

    #[test]
    fn size_classes_are_isolated() {
        let pool = BufferPool::new(65536, 64);
        let large = pool.acquire_sized(4 * 1024 * 1024);
        pool.release(large);

        // A small acquisition must not pop the recycled large buffer.
        let small = pool.acquire_sized(65536);
        assert!(
            small.capacity() < 1024 * 1024,
            "small class must stay isolated, got {}",
            small.capacity()
        );
    }

    #[test]
    fn frozen_buffer_recoverable_after_references_drop() {
        // The seal-worker recycling path relies on Bytes::try_into_mut:
        // the frozen segment buffer converts back to BytesMut, zero-copy,
        // once the sealing-data reference is dropped.
        let pool = BufferPool::new(65536, 4);
        let mut buf = pool.acquire_sized(1024);
        buf.extend_from_slice(b"recycle-me");
        let bytes = buf.freeze();

        let view = bytes.clone();
        assert!(bytes.clone().try_into_mut().is_err(), "shared Bytes must not convert");
        drop(view);

        let recovered = bytes.try_into_mut().expect("unique owner must convert back");
        assert_eq!(&recovered[..], b"recycle-me");
        pool.release(recovered);
    }
}
