//! Load scenario generator — worker framework and statistics collection.
//!
//! This module provides the engine for every Phase 1-4 load test:
//!
//! - [`LoadScenario`] describes the test: concurrency, duration, operation
//!   mix, blob size distribution, key space strategy.
//! - [`Worker`] is a tokio task that loops for the scenario duration,
//!   picks random operations/blobs, executes against the cluster, and
//!   records stats via atomic counters.
//! - [`Orchestrator`] spawns N workers, waits for the duration, and
//!   collects [`AggregateStats`].
//!
//! ## Determinism
//!
//! The [`LoadScenario::seed`] feeds a [`ChaCha12Rng`]. With the same seed,
//! two runs produce identical operation sequences — essential for
//! reproducible performance comparisons.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha12Rng;
use serde::Serialize;

use super::manifest::Manifest;
use crate::harness::{random_bytes, Cluster};

// ---------------------------------------------------------------------------
// LoadScenario
// ---------------------------------------------------------------------------

/// Describes a complete load test configuration.
///
/// The scenario specifies how many concurrent workers, how long to run,
/// what operations to perform, what blob sizes to generate, and how keys
/// are distributed.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use e2e::load::{
///     BlobSizeDist, KeySpace, LoadScenario, OpWeight, Operation,
/// };
///
/// let scenario = LoadScenario {
///     concurrency: 4,
///     duration: Duration::from_secs(30),
///     operations: vec![
///         OpWeight { op: Operation::Put, weight: 0.5 },
///         OpWeight { op: Operation::Get, weight: 0.4 },
///         OpWeight { op: Operation::Delete, weight: 0.05 },
///         OpWeight { op: Operation::Head, weight: 0.05 },
///     ],
///     blob_sizes: BlobSizeDist::Fixed(4096),
///     key_space: KeySpace::RandomUuid,
///     seed: 42,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct LoadScenario {
    /// Number of concurrent worker tasks.
    pub concurrency: usize,
    /// How long the load test runs.
    pub duration: Duration,
    /// Weighted operation mix. Weights are normalized internally.
    pub operations: Vec<OpWeight>,
    /// Blob size distribution for PUT bodies.
    pub blob_sizes: BlobSizeDist,
    /// Key space strategy for generating object keys.
    pub key_space: KeySpace,
    /// Seed for deterministic random number generation.
    /// Each worker derives its seed as `seed + worker_id`.
    pub seed: u64,
}

// ---------------------------------------------------------------------------
// OpWeight
// ---------------------------------------------------------------------------

/// A weighted entry in the operation mix.
///
/// The `weight` is a relative probability. For example, `Weight { op: Put,
/// weight: 0.5 }` and `Weight { op: Get, weight: 0.5 }` means 50% PUTs,
/// 50% GETs.
#[derive(Debug, Clone)]
pub struct OpWeight {
    /// The operation type.
    pub op: Operation,
    /// Relative probability (normalized across all weights).
    pub weight: f64,
}

// ---------------------------------------------------------------------------
// Operation
// ---------------------------------------------------------------------------

/// S3 operations the load generator can perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Operation {
    /// Upload an object.
    Put,
    /// Download an object.
    Get,
    /// Delete an object.
    Delete,
    /// Check object existence (HEAD request).
    Head,
}

// ---------------------------------------------------------------------------
// BlobSizeDist
// ---------------------------------------------------------------------------

/// Blob size distribution for PUT bodies.
///
/// Covers all four segment size tiers:
/// - **inline** (≤4 KB): stored inline in the metadata segment entry.
/// - **small** (4–256 KB): fits in a single small segment.
/// - **standard** (256 KB–4 MB): fits in a standard single segment.
/// - **multi** (>4 MB): spans multiple segments.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BlobSizeDist {
    /// All blobs are exactly this many bytes.
    Fixed(usize),
    /// Blob sizes are uniformly distributed in [min, max].
    Range(usize, usize),
    /// Tiered distribution matching OceanFS segment tiers.
    ///
    /// The percentages are relative weights. Inline is ≤4 KiB,
    /// small is 4–256 KiB, standard is 256 KiB–4 MiB, and multi is
    /// >4 MiB (capped at 16 MiB for practical test purposes).
    Tiered {
        /// Percentage weight for inline blobs (≤4 KiB).
        inline_pct: f64,
        /// Percentage weight for small blobs (4–256 KiB).
        small_pct: f64,
        /// Percentage weight for standard blobs (256 KiB–4 MiB).
        standard_pct: f64,
        /// Percentage weight for multi-segment blobs (>4 MiB, capped at 16 MiB).
        multi_pct: f64,
    },
}

// Tier boundary constants (in bytes).
const INLINE_MAX: usize = 4 * 1024;
const SMALL_MIN: usize = INLINE_MAX + 1;
const SMALL_MAX: usize = 256 * 1024;
const STANDARD_MIN: usize = SMALL_MAX + 1;
const STANDARD_MAX: usize = 4 * 1024 * 1024;
const MULTI_MIN: usize = STANDARD_MAX + 1;
const MULTI_MAX: usize = 16 * 1024 * 1024;

impl BlobSizeDist {
    /// Generates a random blob size from this distribution.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::BlobSizeDist;
    ///
    /// let dist = BlobSizeDist::Fixed(4096);
    /// let mut rng = rand::thread_rng();
    /// assert_eq!(dist.sample(&mut rng), 4096);
    /// ```
    pub fn sample(&self, rng: &mut impl Rng) -> usize {
        match self {
            BlobSizeDist::Fixed(size) => *size,
            BlobSizeDist::Range(min, max) => {
                if min >= max {
                    *min
                } else {
                    rng.gen_range(*min..=*max)
                }
            }
            BlobSizeDist::Tiered { inline_pct, small_pct, standard_pct, multi_pct } => {
                let total = inline_pct + small_pct + standard_pct + multi_pct;
                if total == 0.0 {
                    return 4096; // fallback
                }
                let roll = rng.gen::<f64>() * total;
                let mut cumulative = 0.0;

                cumulative += inline_pct;
                if roll < cumulative {
                    return rng.gen_range(1..=INLINE_MAX);
                }

                cumulative += small_pct;
                if roll < cumulative {
                    return rng.gen_range(SMALL_MIN..=SMALL_MAX);
                }

                cumulative += standard_pct;
                if roll < cumulative {
                    return rng.gen_range(STANDARD_MIN..=STANDARD_MAX);
                }

                // Multi tier.
                rng.gen_range(MULTI_MIN..=MULTI_MAX)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// KeySpace
// ---------------------------------------------------------------------------

/// Key space strategy for generating object keys during a load test.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum KeySpace {
    /// Every key is a random UUID v7.
    RandomUuid,
    /// Sequential keys of the form `{prefix}-{N}` where N ranges
    /// from `start` to `start + count - 1`.
    Sequential {
        /// Prefix for generated keys, e.g., `"obj"`.
        prefix: String,
        /// First sequence number.
        start: u64,
        /// Total number of distinct keys.
        count: u64,
    },
    /// Zipfian distribution over a key space.
    ///
    /// `hot_keys` keys appear more frequently than `cold_keys` keys,
    /// controlled by the `skew` parameter (higher = more skew).
    Zipfian {
        /// Number of "hot" keys in the distribution.
        hot_keys: usize,
        /// Number of "cold" keys (total keys = hot_keys + cold_keys).
        cold_keys: usize,
        /// Skew parameter; 1.0 = classic Zipf, higher = more skew.
        skew: f64,
    },
    /// Random UUID keys, with a fraction from a shared pool for
    /// same-key concurrency testing.
    ///
    /// `shared_ratio` fraction of keys are drawn from a pool of
    /// `shared_pool_size` keys (named `concurrent-0` through
    /// `concurrent-{shared_pool_size-1}`). The remaining fraction
    /// are random UUIDs.
    RandomUuidWithSharedPool {
        /// Number of distinct keys in the shared pool.
        shared_pool_size: usize,
        /// Fraction of keys drawn from the shared pool (0.0–1.0).
        shared_ratio: f64,
    },
}

impl KeySpace {
    /// Generates the next key from this key space.
    pub fn next_key(&self, rng: &mut impl Rng) -> String {
        match self {
            KeySpace::RandomUuid => {
                // Generate a deterministic v4 UUID from the seeded RNG
                // so that the same seed produces the same key sequence.
                let mut bytes = [0u8; 16];
                rng.fill(&mut bytes);
                // Set v4 UUID variant bits per RFC 4122.
                bytes[6] = (bytes[6] & 0x0f) | 0x40;
                bytes[8] = (bytes[8] & 0x3f) | 0x80;
                uuid::Uuid::from_bytes(bytes).to_string()
            }
            KeySpace::Sequential { prefix, start, count } => {
                let n = if *count > 1 {
                    rng.gen_range(*start..start.saturating_add(*count))
                } else {
                    *start
                };
                format!("{prefix}-{n}")
            }
            KeySpace::Zipfian { hot_keys, cold_keys, skew } => {
                zipfian_key(*hot_keys, *cold_keys, *skew, rng)
            }
            KeySpace::RandomUuidWithSharedPool { shared_pool_size, shared_ratio } => {
                let roll = rng.gen::<f64>();
                if roll < *shared_ratio && *shared_pool_size > 0 {
                    let idx = rng.gen_range(0..*shared_pool_size);
                    format!("concurrent-{idx}")
                } else {
                    let mut bytes = [0u8; 16];
                    rng.fill(&mut bytes);
                    bytes[6] = (bytes[6] & 0x0f) | 0x40;
                    bytes[8] = (bytes[8] & 0x3f) | 0x80;
                    uuid::Uuid::from_bytes(bytes).to_string()
                }
            }
        }
    }
}

/// Sample a key from a Zipfian distribution.
///
/// The first `hot_keys` indices are weighted by `1 / (rank^skew)`.
/// The remaining `cold_keys` indices have a small uniform tail weight.
fn zipfian_key(hot_keys: usize, cold_keys: usize, skew: f64, rng: &mut impl Rng) -> String {
    let total_weight = zipf_total_weight(hot_keys, skew);

    // Tail weight per cold key (very small).
    let cold_weight = if total_weight > 0.0 && cold_keys > 0 {
        total_weight * 0.001 / cold_keys as f64
    } else {
        0.0
    };

    let roll = rng.gen::<f64>() * (total_weight + cold_weight * cold_keys as f64);

    let mut cumulative = 0.0;
    for rank in 1..=hot_keys {
        cumulative += 1.0 / (rank as f64).powf(skew);
        if roll < cumulative {
            return format!("hot-{rank}");
        }
    }

    // Fell into cold keys.
    let cold_idx = rng.gen_range(0..cold_keys);
    format!("cold-{cold_idx}")
}

/// Compute the total weight of a Zipfian distribution with `n` items.
fn zipf_total_weight(n: usize, skew: f64) -> f64 {
    let mut total = 0.0;
    for rank in 1..=n {
        total += 1.0 / (rank as f64).powf(skew);
    }
    total
}

// ---------------------------------------------------------------------------
// LatencyHistogram
// ---------------------------------------------------------------------------

const HISTOGRAM_BUCKETS: usize = 32;

/// A bucketed latency histogram using `AtomicU64` counters.
///
/// Each bucket `i` covers the range `[2^i, 2^(i+1))` microseconds.
/// This covers latencies from 1 µs to ~35 minutes, sufficient for any
/// S3 operation.
///
/// Per §11.1 (atomic counters on hot paths), all buckets use `AtomicU64`
/// with `Relaxed` ordering.
pub(crate) struct LatencyHistogram {
    /// Exponential bucket counters. Bucket `i` covers `[2^i, 2^(i+1))` µs.
    buckets: [AtomicU64; HISTOGRAM_BUCKETS],
}

impl LatencyHistogram {
    /// Creates a new, empty latency histogram.
    pub(crate) fn new() -> Self {
        Self { buckets: std::array::from_fn(|_| AtomicU64::new(0)) }
    }

    /// Records a latency observation.
    ///
    /// The duration is converted to microseconds and placed in the
    /// appropriate exponential bucket.
    pub(crate) fn record(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        let idx = bucket_index(micros);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the total number of recorded observations.
    pub(crate) fn count(&self) -> u64 {
        self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }

    /// Computes the approximate `p`-th percentile latency in microseconds.
    ///
    /// `p` should be in `[0.0, 1.0]`. Uses linear interpolation within
    /// the bucket that contains the threshold value.
    pub(crate) fn percentile(&self, p: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let threshold = ((total as f64) * p).ceil() as u64;
        let mut cumulative: u64 = 0;

        for (i, bucket) in self.buckets.iter().enumerate() {
            let count = bucket.load(Ordering::Relaxed);
            cumulative += count;
            if cumulative >= threshold {
                let bucket_start = if i == 0 { 0 } else { 1u64 << i };
                let bucket_end = 1u64 << (i + 1);
                let prev_cumulative = cumulative - count;
                let fraction = (threshold - prev_cumulative) as f64 / count as f64;
                return bucket_start + ((bucket_end - bucket_start) as f64 * fraction) as u64;
            }
        }

        // Fallback: last bucket start.
        1u64 << (HISTOGRAM_BUCKETS - 1)
    }

    /// Merges another histogram into this one by summing bucket counts.
    pub(crate) fn merge(&self, other: &LatencyHistogram) {
        for (a, b) in self.buckets.iter().zip(other.buckets.iter()) {
            a.fetch_add(b.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the bucket index for a given microsecond value.
///
/// Bucket 0: [0, 2) µs
/// Bucket 1: [2, 4) µs
/// Bucket 2: [4, 8) µs
/// ...
/// Bucket 31: [2^31, 2^32) µs ≈ [35 min, 70 min)
fn bucket_index(micros: u64) -> usize {
    if micros <= 1 {
        return 0;
    }
    let idx = 64 - micros.leading_zeros() - 1;
    let idx = idx as usize;
    if idx >= HISTOGRAM_BUCKETS {
        HISTOGRAM_BUCKETS - 1
    } else {
        idx
    }
}

// ---------------------------------------------------------------------------
// WorkerStats
// ---------------------------------------------------------------------------

/// Per-worker atomic counters and latency histograms.
///
/// Each [`Worker`] owns its own `WorkerStats`; there is no sharing
/// between workers. Counters use `AtomicU64` for consistency with
/// perf §11.1, even though each worker accesses its own stats
/// sequentially from a single tokio task.
#[derive(Default)]
pub struct WorkerStats {
    // ── PUT counters ────
    puts_total: AtomicU64,
    puts_200: AtomicU64,
    puts_4xx: AtomicU64,
    puts_5xx: AtomicU64,
    // ── GET counters ────
    gets_total: AtomicU64,
    gets_200: AtomicU64,
    gets_404: AtomicU64,
    // ── DELETE counters ──
    deletes_total: AtomicU64,
    deletes_204: AtomicU64,
    // ── HEAD counters ────
    heads_total: AtomicU64,
    heads_200: AtomicU64,
    // ── Error counter ────
    errors_total: AtomicU64,
    // ── Per-tier blob size counters ──
    puts_inline: AtomicU64,
    puts_small: AtomicU64,
    puts_standard: AtomicU64,
    puts_multi: AtomicU64,
    // ── Latency histograms ─
    put_latency: LatencyHistogram,
    get_latency: LatencyHistogram,
    delete_latency: LatencyHistogram,
    head_latency: LatencyHistogram,
}

impl WorkerStats {
    /// Creates a new, empty `WorkerStats`.
    pub fn new() -> Self {
        Self::default()
    }

    // ── Counter accessors ─────────────────────

    /// Total PUT operations attempted.
    pub fn puts_total(&self) -> u64 {
        self.puts_total.load(Ordering::Relaxed)
    }
    /// Successful PUTs (HTTP 200).
    pub fn puts_200(&self) -> u64 {
        self.puts_200.load(Ordering::Relaxed)
    }
    /// PUTs rejected with an HTTP 4xx status (e.g., 413 body-limit
    /// rejections). These are silent in the old counters — 200/5xx/errors
    /// only — so a run can "pass" while nearly half of its PUTs fail.
    pub fn puts_4xx(&self) -> u64 {
        self.puts_4xx.load(Ordering::Relaxed)
    }
    /// Failed PUTs (HTTP 5xx).
    pub fn puts_5xx(&self) -> u64 {
        self.puts_5xx.load(Ordering::Relaxed)
    }
    /// Total GET operations attempted.
    pub fn gets_total(&self) -> u64 {
        self.gets_total.load(Ordering::Relaxed)
    }
    /// Successful GETs (HTTP 200).
    pub fn gets_200(&self) -> u64 {
        self.gets_200.load(Ordering::Relaxed)
    }
    /// Not-found GETs (HTTP 404).
    pub fn gets_404(&self) -> u64 {
        self.gets_404.load(Ordering::Relaxed)
    }
    /// Total DELETE operations attempted.
    pub fn deletes_total(&self) -> u64 {
        self.deletes_total.load(Ordering::Relaxed)
    }
    /// Successful DELETEs (HTTP 204).
    pub fn deletes_204(&self) -> u64 {
        self.deletes_204.load(Ordering::Relaxed)
    }
    /// Total HEAD operations attempted.
    pub fn heads_total(&self) -> u64 {
        self.heads_total.load(Ordering::Relaxed)
    }
    /// Successful HEADs (HTTP 200).
    pub fn heads_200(&self) -> u64 {
        self.heads_200.load(Ordering::Relaxed)
    }
    /// Total operation errors (transport, timeout, etc.).
    pub fn errors_total(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }
    /// Successful PUTs (HTTP 200) classified as inline tier (≤4 KiB).
    pub fn puts_inline(&self) -> u64 {
        self.puts_inline.load(Ordering::Relaxed)
    }
    /// Successful PUTs (HTTP 200) classified as small tier (4–256 KiB).
    pub fn puts_small(&self) -> u64 {
        self.puts_small.load(Ordering::Relaxed)
    }
    /// Successful PUTs (HTTP 200) classified as standard tier (256 KiB–4 MiB).
    pub fn puts_standard(&self) -> u64 {
        self.puts_standard.load(Ordering::Relaxed)
    }
    /// Successful PUTs (HTTP 200) classified as multi tier (>4 MiB).
    pub fn puts_multi(&self) -> u64 {
        self.puts_multi.load(Ordering::Relaxed)
    }

    // ── Recording helpers ─────────────────────

    /// Records a PUT operation result.
    pub(crate) fn record_put(&self, status: u16, latency: Duration) {
        self.puts_total.fetch_add(1, Ordering::Relaxed);
        if status == 200 {
            self.puts_200.fetch_add(1, Ordering::Relaxed);
        } else if (400..500).contains(&status) {
            self.puts_4xx.fetch_add(1, Ordering::Relaxed);
        } else if status >= 500 {
            self.puts_5xx.fetch_add(1, Ordering::Relaxed);
        }
        self.put_latency.record(latency);
    }

    /// Records a GET operation result.
    pub(crate) fn record_get(&self, status: u16, latency: Duration) {
        self.gets_total.fetch_add(1, Ordering::Relaxed);
        if status == 200 {
            self.gets_200.fetch_add(1, Ordering::Relaxed);
        } else if status == 404 {
            self.gets_404.fetch_add(1, Ordering::Relaxed);
        }
        self.get_latency.record(latency);
    }

    /// Records a DELETE operation result.
    pub(crate) fn record_delete(&self, status: u16, latency: Duration) {
        self.deletes_total.fetch_add(1, Ordering::Relaxed);
        if status == 204 {
            self.deletes_204.fetch_add(1, Ordering::Relaxed);
        }
        self.delete_latency.record(latency);
    }

    /// Records a HEAD operation result.
    pub(crate) fn record_head(&self, status: u16, latency: Duration) {
        self.heads_total.fetch_add(1, Ordering::Relaxed);
        if status == 200 {
            self.heads_200.fetch_add(1, Ordering::Relaxed);
        }
        self.head_latency.record(latency);
    }

    /// Records an operation error (transport failure, timeout, etc.).
    pub(crate) fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the blob size tier for a **successful** PUT operation.
    ///
    /// Callers must invoke this only when the PUT returned HTTP 200 —
    /// the tier counters measure *successful* coverage, not attempts
    /// (an attempt that is rejected by the body limit never exercises
    /// the storage path). Classifies the blob size into one of four
    /// tiers:
    /// - inline (≤4 KiB)
    /// - small (4–256 KiB)
    /// - standard (256 KiB–4 MiB)
    /// - multi (>4 MiB)
    pub(crate) fn record_blob_size_tier(&self, size_bytes: usize) {
        if size_bytes <= INLINE_MAX {
            self.puts_inline.fetch_add(1, Ordering::Relaxed);
        } else if size_bytes <= SMALL_MAX {
            self.puts_small.fetch_add(1, Ordering::Relaxed);
        } else if size_bytes <= STANDARD_MAX {
            self.puts_standard.fetch_add(1, Ordering::Relaxed);
        } else {
            self.puts_multi.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Merges another `WorkerStats` into this one by summing all
    /// counters and histogram buckets.
    fn merge_from(&self, other: &WorkerStats) {
        self.puts_total.fetch_add(other.puts_total(), Ordering::Relaxed);
        self.puts_200.fetch_add(other.puts_200(), Ordering::Relaxed);
        self.puts_4xx.fetch_add(other.puts_4xx(), Ordering::Relaxed);
        self.puts_5xx.fetch_add(other.puts_5xx(), Ordering::Relaxed);
        self.gets_total.fetch_add(other.gets_total(), Ordering::Relaxed);
        self.gets_200.fetch_add(other.gets_200(), Ordering::Relaxed);
        self.gets_404.fetch_add(other.gets_404(), Ordering::Relaxed);
        self.deletes_total.fetch_add(other.deletes_total(), Ordering::Relaxed);
        self.deletes_204.fetch_add(other.deletes_204(), Ordering::Relaxed);
        self.heads_total.fetch_add(other.heads_total(), Ordering::Relaxed);
        self.heads_200.fetch_add(other.heads_200(), Ordering::Relaxed);
        self.errors_total.fetch_add(other.errors_total(), Ordering::Relaxed);
        self.puts_inline.fetch_add(other.puts_inline(), Ordering::Relaxed);
        self.puts_small.fetch_add(other.puts_small(), Ordering::Relaxed);
        self.puts_standard.fetch_add(other.puts_standard(), Ordering::Relaxed);
        self.puts_multi.fetch_add(other.puts_multi(), Ordering::Relaxed);
        self.put_latency.merge(&other.put_latency);
        self.get_latency.merge(&other.get_latency);
        self.delete_latency.merge(&other.delete_latency);
        self.head_latency.merge(&other.head_latency);
    }
}

impl std::fmt::Debug for WorkerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStats")
            .field("puts_total", &self.puts_total())
            .field("puts_200", &self.puts_200())
            .field("puts_4xx", &self.puts_4xx())
            .field("puts_5xx", &self.puts_5xx())
            .field("gets_total", &self.gets_total())
            .field("gets_200", &self.gets_200())
            .field("gets_404", &self.gets_404())
            .field("deletes_total", &self.deletes_total())
            .field("deletes_204", &self.deletes_204())
            .field("heads_total", &self.heads_total())
            .field("heads_200", &self.heads_200())
            .field("errors_total", &self.errors_total())
            .field("puts_inline", &self.puts_inline())
            .field("puts_small", &self.puts_small())
            .field("puts_standard", &self.puts_standard())
            .field("puts_multi", &self.puts_multi())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// AggregateStats
// ---------------------------------------------------------------------------

/// Aggregated statistics across all workers in a load test.
///
/// Computed by merging [`WorkerStats`] from each worker.
/// Provides summary counters and approximate p50/p99 percentiles
/// from the merged latency histograms.
#[derive(Debug, Clone, Serialize)]
pub struct AggregateStats {
    /// Total PUTs attempted.
    pub puts_total: u64,
    /// Successful PUTs (HTTP 200).
    pub puts_200: u64,
    /// PUTs rejected with an HTTP 4xx status (e.g., 413).
    pub puts_4xx: u64,
    /// Failed PUTs (HTTP 5xx).
    pub puts_5xx: u64,
    /// Total GETs attempted.
    pub gets_total: u64,
    /// Successful GETs (HTTP 200).
    pub gets_200: u64,
    /// Not-found GETs (HTTP 404).
    pub gets_404: u64,
    /// Total DELETEs attempted.
    pub deletes_total: u64,
    /// Successful DELETEs (HTTP 204).
    pub deletes_204: u64,
    /// Total HEADs attempted.
    pub heads_total: u64,
    /// Successful HEADs (HTTP 200).
    pub heads_200: u64,
    /// Total operation errors.
    pub errors_total: u64,
    /// Total operations across all types.
    pub ops_total: u64,
    /// PUTs classified as inline tier (≤4 KiB) — successful PUTs only.
    pub puts_inline: u64,
    /// PUTs classified as small tier (4–256 KiB) — successful PUTs only.
    pub puts_small: u64,
    /// PUTs classified as standard tier (256 KiB–4 MiB) — successful PUTs only.
    pub puts_standard: u64,
    /// PUTs classified as multi tier (>4 MiB) — successful PUTs only.
    pub puts_multi: u64,
    /// Number of workers that completed at least one operation.
    ///
    /// Set by the orchestrator after joining workers; a worker that
    /// panicked before its first completed operation is not counted.
    pub active_workers: u64,
    /// PUT latency p50 (microseconds).
    pub put_p50_us: u64,
    /// PUT latency p99 (microseconds).
    pub put_p99_us: u64,
    /// GET latency p50 (microseconds).
    pub get_p50_us: u64,
    /// GET latency p99 (microseconds).
    pub get_p99_us: u64,
    /// DELETE latency p50 (microseconds).
    pub delete_p50_us: u64,
    /// DELETE latency p99 (microseconds).
    pub delete_p99_us: u64,
    /// HEAD latency p50 (microseconds).
    pub head_p50_us: u64,
    /// HEAD latency p99 (microseconds).
    pub head_p99_us: u64,
    /// Actual elapsed wall-clock time of the test.
    pub elapsed_secs: f64,
}

impl AggregateStats {
    /// Merges a collection of [`WorkerStats`] into aggregate statistics.
    ///
    /// Computes p50/p99 from the merged latency histograms.
    pub fn merge(stats: &[WorkerStats]) -> Self {
        let merged = WorkerStats::new();
        for s in stats {
            merged.merge_from(s);
        }

        let puts_total = merged.puts_total();
        let puts_200 = merged.puts_200();
        let puts_4xx = merged.puts_4xx();
        let puts_5xx = merged.puts_5xx();
        let gets_total = merged.gets_total();
        let gets_200 = merged.gets_200();
        let gets_404 = merged.gets_404();
        let deletes_total = merged.deletes_total();
        let deletes_204 = merged.deletes_204();
        let heads_total = merged.heads_total();
        let heads_200 = merged.heads_200();
        let errors_total = merged.errors_total();
        let puts_inline = merged.puts_inline();
        let puts_small = merged.puts_small();
        let puts_standard = merged.puts_standard();
        let puts_multi = merged.puts_multi();
        let ops_total = puts_total + gets_total + deletes_total + heads_total;

        Self {
            puts_total,
            puts_200,
            puts_4xx,
            puts_5xx,
            gets_total,
            gets_200,
            gets_404,
            deletes_total,
            deletes_204,
            heads_total,
            heads_200,
            errors_total,
            ops_total,
            puts_inline,
            puts_small,
            puts_standard,
            puts_multi,
            active_workers: 0, // set by the orchestrator after join
            put_p50_us: merged.put_latency.percentile(0.50),
            put_p99_us: merged.put_latency.percentile(0.99),
            get_p50_us: merged.get_latency.percentile(0.50),
            get_p99_us: merged.get_latency.percentile(0.99),
            delete_p50_us: merged.delete_latency.percentile(0.50),
            delete_p99_us: merged.delete_latency.percentile(0.99),
            head_p50_us: merged.head_latency.percentile(0.50),
            head_p99_us: merged.head_latency.percentile(0.99),
            elapsed_secs: 0.0, // set by Orchestrator
        }
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/// A single load-generator task.
///
/// Each worker loops for the scenario duration, picks random
/// operations/blobs/keys, executes them against the cluster, and
/// records results in its [`WorkerStats`].
pub struct Worker {
    /// Unique worker identifier.
    id: usize,
    /// Reference to the cluster under test.
    cluster: Arc<Cluster>,
    /// Shared manifest for tracking PUT/DELETE operations.
    manifest: Arc<Manifest>,
    /// Read-only scenario configuration.
    scenario: Arc<LoadScenario>,
    /// Per-worker statistics (collected during execution).
    stats: WorkerStats,
    /// Shared activity counter incremented once this worker completes
    /// its first operation (owned by the orchestrator).
    activity: Arc<AtomicU64>,
}

impl Worker {
    /// Creates a new worker.
    ///
    /// `activity` is a shared counter owned by the orchestrator: the
    /// worker increments it exactly once, when its first operation
    /// completes, so the orchestrator can distinguish workers that ran
    /// from workers that panicked before doing any work.
    pub fn new(
        id: usize,
        cluster: Arc<Cluster>,
        manifest: Arc<Manifest>,
        scenario: Arc<LoadScenario>,
        activity: Arc<AtomicU64>,
    ) -> Self {
        Self { id, cluster, manifest, scenario, stats: WorkerStats::new(), activity }
    }

    /// Runs the worker loop until the scenario duration elapses.
    ///
    /// On each tick:
    /// 1. Picks a weighted random operation.
    /// 2. Generates a blob size (for PUTs).
    /// 3. Picks a key from the key space.
    /// 4. Executes the operation against a random cluster node.
    /// 5. Records the result in [`WorkerStats`].
    ///
    /// Returns the collected [`WorkerStats`] on completion.
    pub async fn run(self) -> WorkerStats {
        // Each worker gets a deterministic but unique RNG.
        let mut rng = ChaCha12Rng::seed_from_u64(self.scenario.seed.wrapping_add(self.id as u64));
        let bucket = "load-test";
        let node_count = self.cluster.len();
        let scenario_start = Instant::now();

        // Per-op debug tracing (opt-in via LOAD_TEST_DEBUG=1). Logs the
        // client-side cost breakdown of every operation: blob generation
        // time (PUTs only), total round-trip time, and the HTTP status.
        let debug_trace = std::env::var("LOAD_TEST_DEBUG")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        // Activity is reported once, on the first *completed* operation,
        // so a worker that panics before finishing any op is not counted
        // as active.
        let mut first_op_completed = false;

        loop {
            // ── Check elapsed time ──
            let elapsed = scenario_start.elapsed();
            if elapsed >= self.scenario.duration {
                break;
            }

            // Pick weighted operation.
            let op = Self::pick_operation(&self.scenario.operations, &mut rng);

            // Pick blob size.
            let size = self.scenario.blob_sizes.sample(&mut rng);

            // Pick key.
            let key = self.scenario.key_space.next_key(&mut rng);

            // Pick random node.
            let node_idx = if node_count > 0 { rng.gen_range(0..node_count) } else { 0 };
            let path = format!("/{bucket}/{key}");

            match op {
                Operation::Put => {
                    // Blob generation is timed separately and excluded
                    // from the latency histogram — the timer starts at
                    // the HTTP boundary so the histogram measures
                    // server round-trips, not client-side generation.
                    let gen_start = Instant::now();
                    let body = random_bytes(size);
                    let gen_elapsed = gen_start.elapsed(); // kept for debug trace
                    let start = Instant::now(); // HTTP-only latency
                    match self.cluster.put(node_idx, &path, &body).await {
                        Ok(resp) => {
                            let status = resp.status().as_u16();
                            let _ = resp.bytes().await; // consume body
                            let latency = start.elapsed();
                            if debug_trace {
                                // `total_ms` keeps its original meaning:
                                // generation + HTTP round-trip.
                                eprintln!(
                                    "[worker-{}] PUT {} size={} gen_ms={:.2} total_ms={:.2} status={}",
                                    self.id,
                                    key,
                                    size,
                                    gen_elapsed.as_secs_f64() * 1e3,
                                    (gen_elapsed + latency).as_secs_f64() * 1e3,
                                    status
                                );
                            }
                            if status == 200 {
                                self.manifest.record(bucket, &key, &body);
                                // Tier counters count successes, not
                                // attempts: a 413-rejected PUT never
                                // exercised that tier's storage path.
                                self.stats.record_blob_size_tier(size);
                            }
                            self.stats.record_put(status, latency);
                        }
                        Err(e) => {
                            if debug_trace {
                                // `total_ms` keeps its original meaning:
                                // generation + HTTP round-trip.
                                eprintln!(
                                    "[worker-{}] PUT {} size={} gen_ms={:.2} total_ms={:.2} ERR={}",
                                    self.id,
                                    key,
                                    size,
                                    gen_elapsed.as_secs_f64() * 1e3,
                                    (gen_elapsed + start.elapsed()).as_secs_f64() * 1e3,
                                    e
                                );
                            }
                            self.stats.record_put(0, start.elapsed());
                            self.stats.record_error();
                        }
                    }
                }
                Operation::Get => {
                    let start = Instant::now();
                    match self.cluster.get(node_idx, &path).await {
                        Ok(resp) => {
                            let status = resp.status().as_u16();
                            let _ = resp.bytes().await; // consume body
                            let latency = start.elapsed();
                            if debug_trace {
                                eprintln!(
                                    "[worker-{}] GET  {} total_ms={:.2} status={}",
                                    self.id,
                                    key,
                                    latency.as_secs_f64() * 1e3,
                                    status
                                );
                            }
                            self.stats.record_get(status, latency);
                        }
                        Err(e) => {
                            if debug_trace {
                                eprintln!(
                                    "[worker-{}] GET  {} total_ms={:.2} ERR={}",
                                    self.id,
                                    key,
                                    start.elapsed().as_secs_f64() * 1e3,
                                    e
                                );
                            }
                            self.stats.record_get(0, start.elapsed());
                            self.stats.record_error();
                        }
                    }
                }
                Operation::Delete => {
                    let start = Instant::now();
                    match self.cluster.delete(node_idx, &path).await {
                        Ok(resp) => {
                            let status = resp.status().as_u16();
                            let _ = resp.bytes().await; // consume body
                            let latency = start.elapsed();
                            if debug_trace {
                                eprintln!(
                                    "[worker-{}] DEL  {} total_ms={:.2} status={}",
                                    self.id,
                                    key,
                                    latency.as_secs_f64() * 1e3,
                                    status
                                );
                            }
                            if status == 204 {
                                self.manifest.record_delete(bucket, &key);
                            }
                            self.stats.record_delete(status, latency);
                        }
                        Err(e) => {
                            if debug_trace {
                                eprintln!(
                                    "[worker-{}] DEL  {} total_ms={:.2} ERR={}",
                                    self.id,
                                    key,
                                    start.elapsed().as_secs_f64() * 1e3,
                                    e
                                );
                            }
                            self.stats.record_delete(0, start.elapsed());
                            self.stats.record_error();
                        }
                    }
                }
                Operation::Head => {
                    let start = Instant::now();
                    match self.cluster.head(node_idx, &path).await {
                        Ok(resp) => {
                            let status = resp.status().as_u16();
                            let _ = resp.bytes().await; // consume body
                            let latency = start.elapsed();
                            if debug_trace {
                                eprintln!(
                                    "[worker-{}] HEAD {} total_ms={:.2} status={}",
                                    self.id,
                                    key,
                                    latency.as_secs_f64() * 1e3,
                                    status
                                );
                            }
                            self.stats.record_head(status, latency);
                        }
                        Err(e) => {
                            if debug_trace {
                                eprintln!(
                                    "[worker-{}] HEAD {} total_ms={:.2} ERR={}",
                                    self.id,
                                    key,
                                    start.elapsed().as_secs_f64() * 1e3,
                                    e
                                );
                            }
                            self.stats.record_head(0, start.elapsed());
                            self.stats.record_error();
                        }
                    }
                }
            }

            // The operation completed (any outcome counts as "ran");
            // report activity exactly once.
            if !first_op_completed {
                first_op_completed = true;
                self.activity.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.stats
    }

    /// Picks a weighted random operation from the operation mix.
    fn pick_operation(ops: &[OpWeight], rng: &mut impl Rng) -> Operation {
        if ops.is_empty() {
            return Operation::Get;
        }
        if ops.len() == 1 {
            return ops[0].op;
        }

        let total_weight: f64 = ops.iter().map(|w| w.weight).sum();
        if total_weight == 0.0 {
            return ops[0].op;
        }

        let roll = rng.gen::<f64>() * total_weight;
        let mut cumulative = 0.0;
        for ow in ops {
            cumulative += ow.weight;
            if roll < cumulative {
                return ow.op;
            }
        }

        // Fallback (shouldn't reach here if weights > 0).
        ops[ops.len() - 1].op
    }
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Manages the lifecycle of a load test: spawn workers, wait for
/// duration, collect and aggregate stats.
///
/// Workers self-terminate by tracking elapsed time against
/// `scenario.duration`. The orchestrator sleeps for the duration,
/// then joins all workers with a generous grace period.
pub struct Orchestrator;

impl Orchestrator {
    /// Runs a load scenario against a cluster.
    ///
    /// Spawns `scenario.concurrency` [`Worker`] tasks, sleeps for
    /// `scenario.duration`, joins all workers, and returns
    /// [`AggregateStats`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{sync::Arc, time::Duration};
    /// use e2e::harness::{config_standard, Cluster};
    /// use e2e::load::{
    ///     BlobSizeDist, KeySpace, LoadScenario, Manifest, OpWeight,
    ///     Operation, Orchestrator,
    /// };
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = Arc::new(Cluster::spawn(1, &config_standard()).await?);
    /// let manifest = Arc::new(Manifest::new());
    /// let scenario = LoadScenario {
    ///     concurrency: 4,
    ///     duration: Duration::from_secs(10),
    ///     operations: vec![
    ///         OpWeight { op: Operation::Put, weight: 1.0 },
    ///         OpWeight { op: Operation::Get, weight: 1.0 },
    ///     ],
    ///     blob_sizes: BlobSizeDist::Fixed(1024),
    ///     key_space: KeySpace::RandomUuid,
    ///     seed: 12345,
    /// };
    ///
    /// let stats = Orchestrator::run(scenario, cluster, manifest).await;
    /// assert!(stats.ops_total > 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run(
        scenario: LoadScenario,
        cluster: Arc<Cluster>,
        manifest: Arc<Manifest>,
    ) -> AggregateStats {
        let start = Instant::now();
        let scenario = Arc::new(scenario);

        // Create the bucket so workers have a known path prefix.
        // Workers PUT to /load-test/{key}; this ensures the bucket exists.
        let _ = cluster.put(0, "/load-test", &[]).await;

        // Shared activity counter: each worker increments it once on its
        // first completed operation, so the orchestrator can assert that
        // every worker actually ran.
        let activity = Arc::new(AtomicU64::new(0));

        // Spawn N workers.
        let mut handles = Vec::with_capacity(scenario.concurrency);
        for id in 0..scenario.concurrency {
            let worker = Worker::new(
                id,
                Arc::clone(&cluster),
                Arc::clone(&manifest),
                Arc::clone(&scenario),
                Arc::clone(&activity),
            );
            handles.push(tokio::spawn(async move { worker.run().await }));
        }

        // Wait for the scenario duration.
        if scenario.duration > Duration::ZERO {
            tokio::time::sleep(scenario.duration).await;
        }

        // Collect stats from all workers. Workers self-terminate when
        // elapsed >= scenario.duration, so they should finish shortly
        // after the orchestrator's sleep completes. A generous 30s grace
        // period handles workers stuck in a slow in-flight operation.
        let mut all_stats = Vec::with_capacity(handles.len());
        for handle in handles {
            match tokio::time::timeout(Duration::from_secs(30), handle).await {
                Ok(Ok(stats)) => all_stats.push(stats),
                Ok(Err(e)) => {
                    // Task panicked — log and continue.
                    eprintln!("Worker task panicked: {e}");
                }
                Err(_) => {
                    eprintln!("Worker task timed out during shutdown");
                }
            }
        }

        let mut aggregate = AggregateStats::merge(&all_stats);
        aggregate.elapsed_secs = start.elapsed().as_secs_f64();
        aggregate.active_workers = activity.load(Ordering::Relaxed);
        aggregate
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── BlobSizeDist tests ─────

    #[test]
    fn blob_size_fixed_returns_exact() {
        let dist = BlobSizeDist::Fixed(1234);
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            assert_eq!(dist.sample(&mut rng), 1234);
        }
    }

    #[test]
    fn blob_size_range_within_bounds() {
        let dist = BlobSizeDist::Range(100, 200);
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let size = dist.sample(&mut rng);
            assert!((100..=200).contains(&size), "size {size} out of [100,200]");
        }
    }

    #[test]
    fn blob_size_tiered_hits_all_four_tiers() {
        let dist = BlobSizeDist::Tiered {
            inline_pct: 1.0,
            small_pct: 1.0,
            standard_pct: 1.0,
            multi_pct: 1.0,
        };
        let mut rng = rand::thread_rng();
        let mut inline = 0;
        let mut small = 0;
        let mut standard = 0;
        let mut multi = 0;

        for _ in 0..10000 {
            let size = dist.sample(&mut rng);
            if size <= INLINE_MAX {
                inline += 1;
            } else if size <= SMALL_MAX {
                small += 1;
            } else if size <= STANDARD_MAX {
                standard += 1;
            } else {
                multi += 1;
            }
        }

        // All four tiers should be hit with roughly equal probability.
        assert!(inline > 1500, "inline tier underrepresented: {inline}/10000");
        assert!(small > 1500, "small tier underrepresented: {small}/10000");
        assert!(standard > 1500, "standard tier underrepresented: {standard}/10000");
        assert!(multi > 1500, "multi tier underrepresented: {multi}/10000");
    }

    #[test]
    fn blob_size_tiered_inline_max_4k() {
        let dist = BlobSizeDist::Tiered {
            inline_pct: 1.0,
            small_pct: 0.0,
            standard_pct: 0.0,
            multi_pct: 0.0,
        };
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let size = dist.sample(&mut rng);
            assert!(size <= INLINE_MAX, "inline size {size} > {INLINE_MAX}");
        }
    }

    // ── KeySpace tests ─────

    #[test]
    fn key_space_random_uuid_produces_distinct_keys() {
        let ks = KeySpace::RandomUuid;
        let mut rng = rand::thread_rng();
        let keys: Vec<String> = (0..100).map(|_| ks.next_key(&mut rng)).collect();
        let mut dedup = keys.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(dedup.len(), 100, "all 100 UUID keys should be distinct");
    }

    #[test]
    fn key_space_sequential_produces_expected_format() {
        let ks = KeySpace::Sequential { prefix: "obj".to_string(), start: 10, count: 5 };
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let key = ks.next_key(&mut rng);
            assert!(key.starts_with("obj-"));
            let num: u64 = key.strip_prefix("obj-").unwrap().parse().unwrap();
            assert!((10..15).contains(&num), "sequential key {num} out of [10,15)");
        }
    }

    #[test]
    fn key_space_zipfian_hot_keys_more_frequent() {
        // With 10 hot keys and 90 cold keys, skew=1.0,
        // hot keys should appear much more often than cold keys.
        let ks = KeySpace::Zipfian { hot_keys: 10, cold_keys: 90, skew: 1.0 };
        let mut rng = rand::thread_rng();
        let mut hot_count = 0usize;
        let mut cold_count = 0usize;

        for _ in 0..10000 {
            let key = ks.next_key(&mut rng);
            if key.starts_with("hot-") {
                hot_count += 1;
            } else if key.starts_with("cold-") {
                cold_count += 1;
            }
        }

        assert!(hot_count > cold_count,
            "hot keys ({hot_count}) should be more frequent than cold keys ({cold_count}) with skew=1.0");
    }

    // ── WorkerStats tests ─────

    #[test]
    fn worker_stats_counters_increment_correctly() {
        let stats = WorkerStats::new();
        stats.record_put(200, Duration::from_millis(10));
        stats.record_put(500, Duration::from_millis(20));
        stats.record_get(200, Duration::from_millis(5));
        stats.record_get(404, Duration::from_millis(3));
        stats.record_delete(204, Duration::from_millis(8));
        stats.record_head(200, Duration::from_millis(2));

        assert_eq!(stats.puts_total(), 2);
        assert_eq!(stats.puts_200(), 1);
        assert_eq!(stats.puts_5xx(), 1);
        assert_eq!(stats.gets_total(), 2);
        assert_eq!(stats.gets_200(), 1);
        assert_eq!(stats.gets_404(), 1);
        assert_eq!(stats.deletes_total(), 1);
        assert_eq!(stats.deletes_204(), 1);
        assert_eq!(stats.heads_total(), 1);
        assert_eq!(stats.heads_200(), 1);
    }

    #[test]
    fn worker_stats_put_4xx_counted_separately_from_5xx() {
        let stats = WorkerStats::new();
        stats.record_put(200, Duration::from_millis(10));
        stats.record_put(413, Duration::from_millis(10));
        stats.record_put(404, Duration::from_millis(10));
        stats.record_put(500, Duration::from_millis(10));

        assert_eq!(stats.puts_total(), 4);
        assert_eq!(stats.puts_200(), 1);
        assert_eq!(stats.puts_4xx(), 2);
        assert_eq!(stats.puts_5xx(), 1);
    }

    #[test]
    fn blob_size_tier_classification_matches_boundaries() {
        let stats = WorkerStats::new();
        stats.record_blob_size_tier(INLINE_MAX); // 4 KiB — inline
        stats.record_blob_size_tier(SMALL_MAX); // 256 KiB — small
        stats.record_blob_size_tier(STANDARD_MAX); // 4 MiB — standard
        stats.record_blob_size_tier(MULTI_MAX); // 16 MiB — multi

        assert_eq!(stats.puts_inline(), 1);
        assert_eq!(stats.puts_small(), 1);
        assert_eq!(stats.puts_standard(), 1);
        assert_eq!(stats.puts_multi(), 1);
    }

    // ── AggregateStats tests ─────

    #[test]
    fn aggregate_stats_merge_sums_counters() {
        let s1 = WorkerStats::new();
        s1.record_put(200, Duration::from_millis(10));
        s1.record_put(200, Duration::from_millis(15));
        s1.record_put(413, Duration::from_millis(15));

        let s2 = WorkerStats::new();
        s2.record_put(200, Duration::from_millis(5));
        s2.record_get(200, Duration::from_millis(8));

        let agg = AggregateStats::merge(&[s1, s2]);
        assert_eq!(agg.puts_total, 4);
        assert_eq!(agg.puts_4xx, 1);
        assert_eq!(agg.gets_total, 1);
        assert_eq!(agg.ops_total, 5);
    }

    #[test]
    fn aggregate_stats_p50_p99_from_histogram() {
        let s1 = WorkerStats::new();
        // Record 100 put latencies: 50 at 1ms, 50 at 100ms.
        for _ in 0..50 {
            s1.record_put(200, Duration::from_millis(1));
        }
        for _ in 0..50 {
            s1.record_put(200, Duration::from_millis(100));
        }

        let agg = AggregateStats::merge(&[s1]);
        // p50 should be near lower values (1ms range).
        // Since we have an exponential bucketed histogram, the exact values
        // will be approximate, but p50 should be in the 1ms neighborhood.
        assert!(agg.put_p50_us < 10_000, "p50 should be < 10ms, got {}µs", agg.put_p50_us);
        // p99 should be near the high values (100ms range).
        assert!(agg.put_p99_us > 50_000, "p99 should be > 50ms, got {}µs", agg.put_p99_us);
    }

    #[test]
    fn aggregate_stats_empty_input_returns_zeros() {
        let agg = AggregateStats::merge(&[]);
        assert_eq!(agg.ops_total, 0);
        assert_eq!(agg.put_p50_us, 0);
        assert_eq!(agg.put_p99_us, 0);
    }

    // ── OpWeight tests ─────

    #[test]
    fn pick_operation_respects_weights() {
        let ops = vec![
            OpWeight { op: Operation::Put, weight: 0.9 },
            OpWeight { op: Operation::Get, weight: 0.1 },
        ];
        let mut rng = rand::thread_rng();
        let mut put_count = 0usize;
        let mut get_count = 0usize;

        for _ in 0..1000 {
            match Worker::pick_operation(&ops, &mut rng) {
                Operation::Put => put_count += 1,
                Operation::Get => get_count += 1,
                _ => {}
            }
        }

        // With 0.9/0.1 weights, puts should dominate.
        assert!(put_count > 700, "expected >700 PUTs, got {put_count}");
        assert!(get_count > 30, "expected >30 GETs, got {get_count}");
    }

    // ── Determinism test ─────

    #[test]
    fn deterministic_seeding_same_sequence() {
        // Two scenarios with the same seed should produce identical
        // operation sequences.
        let scenario1 = LoadScenario {
            concurrency: 1,
            duration: Duration::ZERO,
            operations: vec![
                OpWeight { op: Operation::Put, weight: 1.0 },
                OpWeight { op: Operation::Get, weight: 1.0 },
            ],
            blob_sizes: BlobSizeDist::Fixed(1024),
            key_space: KeySpace::Sequential { prefix: "k".to_string(), start: 0, count: 100 },
            seed: 42,
        };

        let scenario2 = LoadScenario { seed: 42, ..scenario1.clone() };

        // Use pick_operation to verify determinism.
        let mut rng1 = ChaCha12Rng::seed_from_u64(scenario1.seed);
        let mut rng2 = ChaCha12Rng::seed_from_u64(scenario2.seed);

        let ops1: Vec<Operation> =
            (0..50).map(|_| Worker::pick_operation(&scenario1.operations, &mut rng1)).collect();
        let ops2: Vec<Operation> =
            (0..50).map(|_| Worker::pick_operation(&scenario2.operations, &mut rng2)).collect();

        assert_eq!(ops1, ops2, "same seed must produce identical operation sequence");
    }

    // ── LatencyHistogram tests ─────

    #[test]
    fn latency_histogram_records_and_counts() {
        let hist = LatencyHistogram::new();
        hist.record(Duration::from_micros(1));
        hist.record(Duration::from_micros(100));
        hist.record(Duration::from_micros(1000));
        hist.record(Duration::from_millis(10));
        assert_eq!(hist.count(), 4);
    }

    #[test]
    fn latency_histogram_empty_percentile_zero() {
        let hist = LatencyHistogram::new();
        assert_eq!(hist.percentile(0.5), 0);
        assert_eq!(hist.percentile(0.99), 0);
    }

    #[test]
    fn latency_histogram_single_value_percentile_equals_that_value() {
        let hist = LatencyHistogram::new();
        hist.record(Duration::from_millis(10));
        // 10ms = 10000µs. This falls in bucket covering [8192, 16384)µs.
        // The p50 should be around 10ms.
        let p50 = hist.percentile(0.5);
        assert!(p50 > 8000 && p50 < 20000, "p50={p50}µs should be near 10000µs");
    }

    #[test]
    fn latency_histogram_merge_sums_buckets() {
        let h1 = LatencyHistogram::new();
        h1.record(Duration::from_millis(1));

        let h2 = LatencyHistogram::new();
        h2.record(Duration::from_millis(2));

        h1.merge(&h2);
        assert_eq!(h1.count(), 2);
    }

    #[test]
    fn bucket_index_boundaries() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1), 0);
        assert_eq!(bucket_index(2), 1);
        assert_eq!(bucket_index(3), 1);
        assert_eq!(bucket_index(4), 2);
        assert_eq!(bucket_index(7), 2);
        assert_eq!(bucket_index(8), 3);
        assert_eq!(bucket_index(1024), 10);
        // Max bucket should cap at 31.
        assert_eq!(bucket_index(u64::MAX), HISTOGRAM_BUCKETS - 1);
    }
}
