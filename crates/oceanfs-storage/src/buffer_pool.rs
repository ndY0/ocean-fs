//! Recyclable buffer pool for segment append operations.
//!
//! Pre-allocates a pool of `BytesMut` buffers and recycles them between
//! segment lifecycles. This avoids repeated allocation pressure during
//! high-throughput write workloads.
//!
//! Per performance guideline §1.2: arena/buffer pool for segment append
//! buffers.

use bytes::BytesMut;
use parking_lot::Mutex;

use crate::error::{Error, Result};

/// A pool of reusable `BytesMut` buffers for active segment writing.
///
/// Buffers are acquired from the pool when a new active segment is
/// created, and released back when the segment is sealed. This
/// amortizes allocation cost to pool initialization.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::BufferPool;
///
/// let pool = BufferPool::new(65536, 4);
/// let mut buf = pool.acquire().unwrap();
/// buf.extend_from_slice(b"hello");
/// pool.release(buf);
/// ```
pub struct BufferPool {
    /// Available buffers ready for acquisition.
    free: Mutex<Vec<BytesMut>>,
    /// Size of each buffer chunk in bytes.
    chunk_size: usize,
    /// Maximum number of buffers in the pool.
    max_buffers: usize,
    /// Total number of buffers created (in-use + free).
    total_created: usize,
}

impl BufferPool {
    /// Creates a new buffer pool.
    ///
    /// `chunk_size` is the size of each buffer in bytes.
    /// `max_buffers` is the maximum number of buffers to keep in the pool.
    pub fn new(chunk_size: usize, max_buffers: usize) -> Self {
        let free = {
            let mut v = Vec::with_capacity(max_buffers);
            // Pre-allocate all buffers eagerly.
            for _ in 0..max_buffers {
                v.push(BytesMut::with_capacity(chunk_size));
            }
            v
        };
        Self { free: Mutex::new(free), chunk_size, max_buffers, total_created: max_buffers }
    }

    /// Acquires a buffer from the pool.
    ///
    /// Returns `BufferPoolExhausted` if no free buffers are available.
    ///
    /// # Errors
    ///
    /// Returns [`Error::BufferPoolExhausted`] when the pool has no free buffers.
    pub fn acquire(&self) -> Result<BytesMut> {
        let mut free = self.free.lock();
        free.pop().ok_or(Error::BufferPoolExhausted)
    }

    /// Releases a buffer back to the pool for reuse.
    ///
    /// If the pool already has `max_buffers` free entries, the buffer
    /// is dropped instead of being stored.
    pub fn release(&self, mut buf: BytesMut) {
        buf.clear();
        let mut free = self.free.lock();
        if free.len() < self.max_buffers {
            free.push(buf);
        }
        // Else: drop the excess buffer to avoid unbounded growth.
    }

    /// Returns the number of free buffers currently available.
    pub fn free_count(&self) -> usize {
        self.free.lock().len()
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn acquire_returns_pre_allocated_buffer() {
        let pool = BufferPool::new(1024, 2);
        let buf = pool.acquire().unwrap();
        assert_eq!(buf.capacity(), 1024);
        assert!(buf.is_empty());
    }

    #[test]
    fn acquire_all_exhausts_pool() {
        let pool = BufferPool::new(1024, 2);
        let _b1 = pool.acquire().unwrap();
        let _b2 = pool.acquire().unwrap();
        assert!(pool.acquire().is_err());
    }

    #[test]
    fn release_makes_buffer_available_again() {
        let pool = BufferPool::new(1024, 2);
        let b1 = pool.acquire().unwrap();
        assert_eq!(pool.free_count(), 1);
        pool.release(b1);
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn release_clears_buffer() {
        let pool = BufferPool::new(1024, 1);
        let mut buf = pool.acquire().unwrap();
        buf.extend_from_slice(&[1, 2, 3]);
        pool.release(buf);
        let reused = pool.acquire().unwrap();
        assert!(reused.is_empty());
    }

    #[test]
    fn release_beyond_max_drops_buffer() {
        let pool = BufferPool::new(1024, 1);
        let b1 = pool.acquire().unwrap();
        // Acquire another (the pool creates one on demand? No — it was
        // pre-allocated. Let's take the only slot, release it, acquire
        // it again, then release a second buffer.
        pool.release(b1);
        let _b2 = pool.acquire().unwrap(); // pool is now empty
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
        let _b = pool.acquire().unwrap();
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
}
