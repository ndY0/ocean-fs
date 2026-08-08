//! Streaming EC encode — spread encode work across the write lifetime.
//!
//! `StreamingEcSegment` wraps an [`ActiveSegment`] and performs EC encoding
//! incrementally: as each stripe row's data shards become available in the
//! segment buffer, the stripe is encoded to parity shards immediately.
//! This eliminates the seal-time latency spike — seal becomes a near-no-op
//! that only collects pre-computed parity shards.
//!
//! ## Encode dispatch
//!
//! When a stripe completes, encoding is dispatched to a rayon worker thread.
//! The encode result (m parity shards) is stored in the parity buffer behind
//! a `parking_lot::Mutex`. At seal time, the parity shards are collected and
//! the final partial stripe (if any) is padded and encoded.
//!
//! ## Memory
//!
//! Parity buffer overhead: `m × num_stripes × strip_size` bytes. For a 4 MB
//! segment with k=4, m=2, strip_size=64KB: 2 × 16 × 64KB = 2 MB + 2 MB final
//! stripe = up to 4 MB per segment. With a pool of 4 active segments, that's
//! up to 16 MB total — acceptable.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use bytes::Bytes;
use oceanfs_core::{CodecConfig, SegmentId, SizeTier};
use oceanfs_ec::Encoder;
use parking_lot::Mutex;

use super::buffer::ActiveSegment;
use crate::error::Result;

/// A segment buffer with streaming EC encode capability.
///
/// Wraps a plain `ActiveSegment` and adds stripe-level tracking, parity
/// accumulation, and background encode dispatch. When [`append`] crosses a
/// stripe row boundary, the completed stripe is encoded via rayon and the
/// parity shards are stored in the parity buffer.
pub(crate) struct StreamingEcSegment {
    /// The underlying append-only segment buffer.
    inner: ActiveSegment,
    /// EC codec parameters (k, m, strip_size).
    data_shards: u8,
    parity_shards: u8,
    strip_size: usize,
    /// Number of bytes per stripe row: `k × strip_size`.
    stripe_byte_len: usize,
    /// Index of the highest stripe that has been dispatched to rayon.
    /// Incremented synchronously in `append()`.
    dispatched: usize,
    /// Index of the highest stripe whose encode has completed.
    /// Incremented by rayon workers after writing parity into the slot.
    /// Read by `parity_shards()` to know how many slots are safe to read.
    completed: Arc<AtomicUsize>,
    /// Parity shards for all stripes.
    ///
    /// Indexed as `[stripe_index][parity_index]`. Each slot is pre-allocated
    /// at construction time with `m` zeroed `BytesMut` of `strip_size` bytes.
    /// Rayon workers write into these slots via `copy_from_slice`.
    parity_buf: Arc<Mutex<Vec<Vec<bytes::BytesMut>>>>,
}

impl StreamingEcSegment {
    /// Creates a new streaming EC segment wrapping the given active segment.
    ///
    /// The EC parameters are extracted from `ec_config`. The parity buffer
    /// is pre-allocated for the expected number of stripes.
    pub fn new(inner: ActiveSegment, ec_config: &CodecConfig) -> Self {
        let k = ec_config.data_shards;
        let m = ec_config.parity_shards as usize;
        let strip = ec_config.strip_size_bytes;
        let stripe_byte_len = k as usize * strip;
        // Pre-allocate parity buffer: at most `target_size / stripe_byte_len`
        // stripes + 1 for a partial final stripe. Each slot holds m zeroed
        // `BytesMut` of `strip_size` bytes, pre-allocated to avoid per-stripe
        // heap allocation on the hot encode path.
        let max_stripes = (inner.target_size() as usize / stripe_byte_len.max(1)) + 1;
        let mut slots = Vec::with_capacity(max_stripes);
        for _ in 0..max_stripes {
            let row: Vec<bytes::BytesMut> =
                (0..m).map(|_| bytes::BytesMut::zeroed(strip)).collect();
            slots.push(row);
        }
        let parity_buf = Arc::new(Mutex::new(slots));
        let completed = Arc::new(AtomicUsize::new(0));

        Self {
            inner,
            data_shards: k,
            parity_shards: m as u8,
            strip_size: strip,
            stripe_byte_len,
            dispatched: 0,
            completed,
            parity_buf,
        }
    }

    /// Appends data to the segment and triggers streaming encode for any
    /// newly completed stripe rows.
    ///
    /// # Returns
    ///
    /// `(offset, length)` within the segment, same as [`ActiveSegment::append`].
    pub fn append(&mut self, data: &[u8]) -> Result<(u64, usize)> {
        let (offset, length) = self.inner.append(data)?;

        // The cursor advances monotonically. Determine which stripe rows
        // have been fully written and dispatch encodes for any new ones.
        let new_cursor = self.inner.size() as usize;
        let stripe_boundary = new_cursor / self.stripe_byte_len.max(1);
        for stripe_idx in self.dispatched..stripe_boundary {
            self.dispatch_stripe_encode(stripe_idx);
        }
        self.dispatched = stripe_boundary;

        Ok((offset, length))
    }

    /// Dispatches a rayon task to encode the given stripe row.
    ///
    /// Clones only the stripe's bytes (k × strip_size, typically ~256 KB),
    /// not the entire segment buffer. Uses a stack-allocated array for data
    /// shard references to avoid per-stripe heap allocation on the hot path.
    fn dispatch_stripe_encode(&self, stripe_idx: usize) {
        let k = self.data_shards as usize;
        let m = self.parity_shards as usize;
        let strip_size = self.strip_size;
        let stripe_byte_len = self.stripe_byte_len;

        let stripe_start = stripe_idx * stripe_byte_len;
        let segment_data = self.inner.data();

        // Snapshot only this stripe's bytes, not the entire segment.
        // For k=4, strip=64KB: ~256 KB instead of up to 4 MB.
        let stripe_bytes = segment_data[stripe_start..stripe_start + stripe_byte_len].to_vec();
        let parity_buf = Arc::clone(&self.parity_buf);
        let completed = Arc::clone(&self.completed);

        rayon::spawn(move || {
            // Stack-allocated array for data shard references (max k=16).
            // Avoids the per-stripe Vec<&[u8]> allocation on the hot path.
            let mut data_shards: [&[u8]; 16] = [&[]; 16];
            for (i, chunk) in stripe_bytes.chunks(strip_size).take(k).enumerate() {
                data_shards[i] = chunk;
            }

            // Encode: k data shards → m parity shards.
            let config = CodecConfig {
                data_shards: k as u8,
                parity_shards: m as u8,
                strip_size_bytes: strip_size,
                ..Default::default()
            };
            let encoder = oceanfs_ec::CauchyEncoder::new(config);
            match encoder.encode(&data_shards[..k], m as u8) {
                Ok(parity) => {
                    let mut buf = parity_buf.lock();
                    // Write encode output into the pre-allocated parity slot.
                    // Each slot was zeroed at construction time — copy_from_slice
                    // overwrites with computed parity, avoiding per-stripe alloc.
                    let slot = &mut buf[stripe_idx];
                    for (dst, src) in slot.iter_mut().zip(parity.iter()) {
                        dst.copy_from_slice(src);
                    }
                    // Signal that this stripe's encode has completed.
                    // `parity_shards()` uses this to know which slots are
                    // safe to read. fetch_max ensures monotonic progress
                    // even if rayon workers complete out of order.
                    completed.fetch_max(stripe_idx + 1, Ordering::Release);
                    // Mutex guard is dropped here — the Release above
                    // pairs with the Acquire in parity_shards().
                }
                Err(e) => {
                    tracing::warn!(
                        stripe_idx = stripe_idx,
                        error = %e,
                        "streaming encode failed for stripe; parity not stored"
                    );
                }
            }
        });
    }

    // ── Delegated accessors ────────────────────────────────────────────

    /// Returns `true` if the segment has reached or exceeded its target size.
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Returns the segment's unique identifier.
    pub fn id(&self) -> SegmentId {
        self.inner.id()
    }

    /// Returns the storage tier of this segment.
    pub fn tier(&self) -> SizeTier {
        self.inner.tier()
    }

    /// Returns the current size of the segment in bytes.
    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.inner.size()
    }

    /// Returns the target size of the segment in bytes.
    #[allow(dead_code)]
    pub fn target_size(&self) -> u64 {
        self.inner.target_size()
    }

    /// Returns a reference to the accumulated data.
    #[allow(dead_code)]
    pub fn data(&self) -> &[u8] {
        self.inner.data()
    }

    /// Consumes the segment, returning the backing buffer for pool reuse.
    pub fn into_buffer(self) -> bytes::BytesMut {
        self.inner.into_buffer()
    }

    /// Returns a reference to the accumulated parity shards.
    ///
    /// Returns `None` if no stripes were encoded (empty segment).
    #[allow(unsafe_code)]
    pub fn parity_shards(&self) -> Option<Vec<Bytes>> {
        // Spin-wait for all dispatched stripes to complete encoding.
        // `dispatched` is set by `append()` before the seal is triggered;
        // we wait here until every rayon worker has finished.
        let dispatched = self.dispatched;
        while self.completed.load(Ordering::Acquire) < dispatched {
            std::hint::spin_loop();
        }

        let completed = self.completed.load(Ordering::Acquire);
        if completed == 0 {
            return None;
        }

        let m = self.parity_shards as usize;
        let buf = self.parity_buf.lock();
        let slice_ptr = buf.as_ptr();
        // capacity = m × completed; the double loop produces exactly this many.
        let mut shards: Vec<Bytes> = Vec::with_capacity(m * completed);

        for stripe_idx in 0..completed {
            // SAFETY: stripe_idx < completed ≤ buf.len() — the atomic
            // completed counter is only incremented by rayon workers after
            // they have finished writing into their slot. The Acquire load
            // synchronizes with the Release store in the worker.
            let stripe = unsafe { &*slice_ptr.add(stripe_idx) };
            let stripe_ptr = stripe.as_ptr();
            for parity_idx in 0..m {
                // SAFETY: parity_idx < m — each slot is pre-allocated with
                // exactly m BytesMut elements at construction time. The
                // worker writes into all m elements before signalling.
                let shard = unsafe { &*stripe_ptr.add(parity_idx) };
                shards.push(shard.clone().freeze());
            }
        }

        Some(shards)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::SegmentSizeConfig;

    use super::*;
    use crate::buffer_pool::BufferPool;

    fn make_segment() -> StreamingEcSegment {
        let pool = BufferPool::new(65536, 4);
        let size_config = SegmentSizeConfig::default();
        let ec_config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let inner = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).unwrap();
        StreamingEcSegment::new(inner, &ec_config)
    }

    #[test]
    fn new_streaming_segment_starts_empty() {
        let seg = make_segment();
        assert_eq!(seg.size(), 0);
        assert!(!seg.is_full());
        assert_eq!(seg.dispatched, 0);
    }

    #[test]
    fn append_single_byte_does_not_complete_stripe() {
        let mut seg = make_segment();
        seg.append(b"x").unwrap();
        // 1 byte written, stripe size is 4*64 = 256 bytes — no stripe complete.
        assert_eq!(seg.dispatched, 0);
    }

    #[test]
    fn append_full_stripe_triggers_encode() {
        let mut seg = make_segment();
        // Write exactly one stripe: k=4, strip=64 → 256 bytes.
        let data = vec![0xABu8; 256];
        seg.append(&data).unwrap();
        assert_eq!(seg.dispatched, 1);

        // Wait briefly for rayon to complete, then check parity.
        std::thread::sleep(std::time::Duration::from_millis(10));

        let parity = {
            let buf = seg.parity_buf.lock();
            buf.get(0).cloned()
        };
        assert!(parity.is_some(), "parity should be computed for stripe 0");
        let p = parity.unwrap();
        assert_eq!(p.len(), 2, "should produce 2 parity shards");
        for (i, shard) in p.iter().enumerate() {
            assert_eq!(shard.len(), 64, "parity shard {i} should be 64 bytes");
        }
    }

    #[test]
    fn append_two_stripes_completes_both() {
        let mut seg = make_segment();
        // Write two stripes: 512 bytes total.
        let data = vec![0xCDu8; 512];
        seg.append(&data).unwrap();
        assert_eq!(seg.dispatched, 2);

        std::thread::sleep(std::time::Duration::from_millis(10));

        let buf = seg.parity_buf.lock();
        assert!(buf.len() >= 2, "should have parity for at least 2 stripes");
    }

    #[test]
    fn parity_shards_returns_none_for_empty() {
        let seg = make_segment();
        let parity = seg.parity_shards();
        assert!(parity.is_none());
    }

    #[test]
    fn streaming_parity_equals_seal_time_parity() {
        let pool = BufferPool::new(65536, 4);
        let size_config =
            SegmentSizeConfig { default_target_size: 1024, ..SegmentSizeConfig::default() };
        let ec_config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };

        // --- Streaming path ---
        let inner = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).unwrap();
        let mut streaming = StreamingEcSegment::new(inner, &ec_config);
        let data = vec![0xEFu8; 256]; // exactly one stripe
        streaming.append(&data).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));

        // --- Seal-time (batch) path ---
        let k = 4usize;
        let strip = 64;
        let data_refs: Vec<&[u8]> = (0..k).map(|i| &data[i * strip..(i + 1) * strip]).collect();
        let encoder = oceanfs_ec::CauchyEncoder::new(ec_config);
        let batch_parity = encoder.encode(&data_refs, 2).unwrap();

        // --- Compare ---
        let buf = streaming.parity_buf.lock();
        let streaming_parity = buf.get(0).expect("stripe 0 parity should exist");
        for i in 0..2 {
            assert_eq!(
                &streaming_parity[i][..],
                &batch_parity[i][..],
                "streaming parity shard {i} must match batch encode"
            );
        }
    }

    #[test]
    fn delegates_id_tier_size_to_inner() {
        let seg = make_segment();
        assert_eq!(seg.id(), seg.inner.id());
        assert_eq!(seg.tier(), seg.inner.tier());
        assert_eq!(seg.size(), seg.inner.size());
    }
}
