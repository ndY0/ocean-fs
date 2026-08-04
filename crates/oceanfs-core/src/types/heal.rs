//! Heal pipeline types.
//!
//! Types for the EC heal dispatch pipeline: `HealRequest` (corrupt shard
//! repair request), `HealStats` (atomic diagnostic counters), and
//! `ShardIndex` (index into a k+m shard set).

use super::id::SegmentId;

// ---------------------------------------------------------------------------
// ShardIndex
// ---------------------------------------------------------------------------

/// Index into a k+m shard set.
///
/// Data shards are numbered 0..k-1; parity shards are numbered k..k+m-1.
///
/// # Examples
///
/// ```
/// use oceanfs_core::ShardIndex;
///
/// let data_shard = ShardIndex(0);
/// let parity_shard = ShardIndex(4);
/// assert_eq!(data_shard.value(), 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardIndex(pub u8);

impl ShardIndex {
    /// Returns the raw shard index value.
    pub fn value(&self) -> u8 {
        self.0
    }
}

impl From<u8> for ShardIndex {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<ShardIndex> for u8 {
    fn from(value: ShardIndex) -> Self {
        value.0
    }
}

// ---------------------------------------------------------------------------
// HealStats
// ---------------------------------------------------------------------------

/// Atomic statistics for the heal pipeline.
///
/// All counters use [`std::sync::atomic::Ordering::Relaxed`] because precise
/// ordering is not required for diagnostic counters — only approximate
/// observability matters (perf rule 11.1).
///
/// # Examples
///
/// ```
/// use oceanfs_core::HealStats;
///
/// let stats = HealStats::default();
/// assert_eq!(stats.heals_attempted(), 0);
/// ```
#[derive(Debug, Default)]
pub struct HealStats {
    /// Total number of heal attempts (includes retries).
    heals_attempted: std::sync::atomic::AtomicU64,
    /// Heals that completed successfully.
    heals_succeeded: std::sync::atomic::AtomicU64,
    /// Heals that exhausted all retries and failed.
    heals_failed: std::sync::atomic::AtomicU64,
    /// Total bytes repaired across all successful heals.
    bytes_repaired: std::sync::atomic::AtomicU64,
}

impl HealStats {
    /// Returns the total number of heal attempts.
    pub fn heals_attempted(&self) -> u64 {
        self.heals_attempted.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the number of successful heal completions.
    pub fn heals_succeeded(&self) -> u64 {
        self.heals_succeeded.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the number of heals that failed after exhausting retries.
    pub fn heals_failed(&self) -> u64 {
        self.heals_failed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the total bytes repaired across all successful heals.
    pub fn bytes_repaired(&self) -> u64 {
        self.bytes_repaired.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Increments the attempts counter by one.
    pub fn inc_attempted(&self) {
        self.heals_attempted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increments the succeeded counter by one.
    pub fn inc_succeeded(&self) {
        self.heals_succeeded.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increments the failed counter by one.
    pub fn inc_failed(&self) {
        self.heals_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Adds the given number of bytes to the repaired counter.
    pub fn add_bytes_repaired(&self, bytes: u64) {
        self.bytes_repaired.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Creates a new [`HealStats`] with all counters initialized to zero.
    pub fn new() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// HealRequest
// ---------------------------------------------------------------------------

/// A request to repair one or more corrupt shards of a segment.
///
/// Submitted to the `HealQueue` by Scrub and Anti-Entropy when
/// corruption is detected. The `HealWorker` drains these requests
/// and coordinates EC-based repair.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{HealRequest, SegmentId};
///
/// let request = HealRequest {
///     segment_id: SegmentId::new(),
///     corrupt_shard_indices: vec![2],
///     retry_count: 0,
/// };
/// assert_eq!(request.retry_count, 0);
/// ```
#[derive(Debug, Clone)]
pub struct HealRequest {
    /// The segment that needs repair.
    pub segment_id: SegmentId,
    /// Indices of the corrupt shards within the k+m shard set.
    pub corrupt_shard_indices: Vec<usize>,
    /// Number of previous attempts (0 = first attempt).
    pub retry_count: u32,
}
