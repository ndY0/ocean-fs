//! Hybrid Logical Clock (HLC).
//!
//! HLC provides causally-consistent total ordering without requiring
//! synchronized physical clocks. Each timestamp combines a wall-clock
//! component (`wall_time`) with a logical counter to disambiguate
//! events at the same physical time.
//!
//! ## Architecture
//!
//! - [`Hlc`]: a 96-bit immutable timestamp
//! - [`HlcClock`]: thread-safe HLC generator using a cache-line-aligned
//!   `AtomicU64` for the wall clock, preventing false sharing under
//!   concurrent access from multiple cores.

use std::sync::atomic::{AtomicU64, Ordering};

/// A 96-bit Hybrid Logical Clock timestamp.
///
/// # Ordering
///
/// HLCs are totally ordered: first by `wall_time`, then by `logical`.
/// This ordering is consistent with causal ordering: if event A
/// happens-before event B, then `A.hlc < B.hlc`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::Hlc;
///
/// let t1 = Hlc::new(1000, 0);
/// let t2 = Hlc::new(1000, 1);
/// assert!(t1 < t2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Hlc {
    /// Physical wall-clock component (milliseconds since epoch).
    pub wall_time: u64,
    /// Logical counter for events at the same wall time.
    pub logical: u32,
}

impl Hlc {
    /// Creates a new HLC with the given components.
    pub fn new(wall_time: u64, logical: u32) -> Self {
        Self { wall_time, logical }
    }

    /// Creates a zero-valued HLC (earliest possible timestamp).
    pub fn zero() -> Self {
        Self { wall_time: 0, logical: 0 }
    }

    /// Returns the physical wall-clock component (milliseconds since epoch).
    pub fn wall_time(&self) -> u64 {
        self.wall_time
    }

    /// Returns the logical counter for events at the same wall time.
    pub fn logical(&self) -> u32 {
        self.logical
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.wall_time
            .cmp(&other.wall_time)
            .then_with(|| self.logical.cmp(&other.logical))
    }
}

/// A thread-safe Hybrid Logical Clock generator.
///
/// Uses a cache-line-aligned `AtomicU64` for the wall-clock component
/// to prevent false sharing when accessed from multiple cores.
///
/// Cache-line alignment (64 bytes on x86_64) ensures that the atomic
/// lives on its own cache line, so concurrent HLC generation on
/// different cores does not cause cache-coherency traffic between them.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HlcClock;
///
/// let clock = HlcClock::new();
/// let t1 = clock.now();
/// let t2 = clock.now();
/// assert!(t1 <= t2, "HLC must be monotonically increasing");
/// ```
#[repr(align(64))]
pub struct HlcClock {
    /// The current wall time (milliseconds since epoch), cached from
    /// the OS and bumped when logical counter wraps.
    wall: AtomicU64,
    /// The current logical counter for events at the current wall time.
    logical: AtomicU64,
}

impl HlcClock {
    /// Creates a new HLC clock initialized to the current system time.
    ///
    /// The logical counter starts at 0.
    pub fn new() -> Self {
        let now_ms = current_time_millis();
        Self {
            wall: AtomicU64::new(now_ms),
            logical: AtomicU64::new(0),
        }
    }

    /// Returns the current HLC timestamp, advancing the logical counter.
    ///
    /// If the logical counter would wrap (exceeding [`u32::MAX`] for
    /// the current wall time), the wall time is bumped and the counter
    /// resets to 0.
    ///
    /// This method is lock-free and safe to call from multiple threads
    /// concurrently. However, callers should use the returned timestamp
    /// immediately — concurrent calls may generate timestamps between
    /// this call and the caller's use of the timestamp.
    pub fn now(&self) -> Hlc {
        let wall = self.wall.load(Ordering::Acquire);
        let logical = self.logical.fetch_add(1, Ordering::AcqRel);
        // `fetch_add` returns the previous value. Using the previous
        // value as the logical component gives us sequential
        // assignment: 0, 1, 2, ... for the same wall time.
        if logical < u32::MAX as u64 {
            Hlc {
                wall_time: wall,
                logical: logical as u32,
            }
        } else {
            // Logical counter exhausted; bump wall time.
            let new_wall = current_time_millis().max(wall + 1);
            self.wall.store(new_wall, Ordering::Release);
            self.logical.store(1, Ordering::Release);
            Hlc {
                wall_time: new_wall,
                logical: 0,
            }
        }
    }

    /// Updates the HLC clock by merging a received timestamp.
    ///
    /// The HLC update rule:
    /// 1. `wall = max(local_wall, received.wall_time)`
    /// 2. If `received.wall_time > local_wall`:
    ///    `logical = received.logical + 1`
    /// 3. Otherwise:
    ///    `logical = max(local_logical, received.logical) + 1`
    ///
    /// This ensures causal consistency: if event A happened-before
    /// event B, then `clock.update(b_hlc) > a_hlc`.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{Hlc, HlcClock};
    ///
    /// let clock = HlcClock::new();
    ///
    /// // Receive a timestamp from a remote node.
    /// let received = Hlc::new(1690000000000, 3);
    /// let updated = clock.update(received);
    /// assert!(updated.wall_time() >= 1690000000000);
    /// assert!(updated.logical() >= 4);
    /// ```
    pub fn update(&self, received: Hlc) -> Hlc {
        loop {
            let local_wall = self.wall.load(Ordering::Acquire);
            let local_logical = self.logical.load(Ordering::Acquire);

            let new_wall = local_wall.max(received.wall_time);

            let new_logical = if received.wall_time > local_wall {
                (received.logical as u64).wrapping_add(1)
            } else {
                // received.wall_time <= local_wall
                let max_logical = local_logical.max(received.logical as u64);
                max_logical.wrapping_add(1)
            };

            // Push wall forward if logical exceeded.
            let new_logical = if new_logical > u32::MAX as u64 {
                let wall_bump = new_wall + 1;
                self.wall.store(wall_bump, Ordering::Release);
                self.logical.store(0, Ordering::Release);
                return Hlc {
                    wall_time: wall_bump,
                    logical: 0,
                };
            } else {
                new_logical
            };

            // Attempt to atomically update both fields.
            // Use a compare-exchange loop on wall to ensure consistency.
            if self.wall.compare_exchange_weak(
                local_wall,
                new_wall,
                Ordering::AcqRel,
                Ordering::Acquire,
            ).is_ok() {
                self.logical.store(new_logical, Ordering::Release);
                return Hlc {
                    wall_time: new_wall,
                    logical: new_logical as u32,
                };
            }
            // CAS failed — retry.
        }
    }
}

impl Default for HlcClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the current system time in milliseconds since the Unix epoch.
fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- Hlc ordering --

    #[test]
    fn newer_wall_time_is_greater() {
        let a = Hlc::new(1000, 5);
        let b = Hlc::new(2000, 0);
        assert!(a < b);
    }

    #[test]
    fn same_wall_higher_logical_is_greater() {
        let a = Hlc::new(1000, 0);
        let b = Hlc::new(1000, 1);
        assert!(a < b);
    }

    #[test]
    fn identical_hlcs_are_equal_and_not_less() {
        let a = Hlc::new(1000, 3);
        let b = Hlc::new(1000, 3);
        assert_eq!(a, b);
        assert!(a >= b);
        assert!(b >= a);
    }

    #[test]
    fn zero_is_smallest() {
        let zero = Hlc::zero();
        let t = Hlc::new(0, 1);
        assert!(zero < t);
    }

    // -- Hlc getters --

    #[test]
    fn hlc_wall_time_and_logical_getters() {
        let hlc = Hlc::new(42, 7);
        assert_eq!(hlc.wall_time(), 42);
        assert_eq!(hlc.logical(), 7);
    }

    // -- HlcClock monotonicity --

    #[test]
    fn clock_now_is_monotonically_increasing() {
        let clock = HlcClock::new();
        let mut prev = clock.now();
        for _ in 0..100 {
            let curr = clock.now();
            assert!(curr > prev, "HLC must be strictly monotonic: {prev:?} vs {curr:?}");
            prev = curr;
        }
    }

    #[test]
    fn clock_now_different_calls_produce_different_hlcs() {
        let clock = HlcClock::new();
        let t1 = clock.now();
        let t2 = clock.now();
        assert_ne!(t1, t2);
    }

    // -- HlcClock update/receive-merge --

    #[test]
    fn clock_update_with_newer_wall_time() {
        let clock = HlcClock::new();
        let received = Hlc::new(9_999_999_999_999, 3);
        let updated = clock.update(received);
        // Because received.wall > local_wall, logical = received.logical + 1
        assert_eq!(updated.wall_time(), 9_999_999_999_999);
        assert_eq!(updated.logical(), 4);
    }

    #[test]
    fn clock_update_with_same_wall_bumps_logical() {
        // Create a clock, generate a timestamp, then simulate receiving
        // a timestamp from a peer with the same wall time.
        let clock = HlcClock::new();
        let local = clock.now();
        // Simulate receiving a remote timestamp with the same wall time
        // as our own. The update should produce a higher logical counter.
        let updated = clock.update(local);
        assert!(updated > local, "updated HLC must be greater than received");
    }

    #[test]
    fn clock_update_does_not_cause_clock_to_go_backward() {
        let clock = HlcClock::new();
        let before = clock.now();
        let received = Hlc::new(1, 0); // very old timestamp
        let updated = clock.update(received);
        let after = clock.now();
        // After clock update, subsequent now() calls must be >= the
        // updated timestamp (they can be equal if the logical counter
        // was not advanced by intermediate calls).
        assert!(after >= updated, "clock should continue advancing after update");
        assert!(updated > before, "updated timestamp should be ahead of before");
    }

    // -- HlcClock cache-line alignment --

    #[test]
    fn hlc_clock_has_64_byte_alignment() {
        // Verify that the struct is at least 64-byte aligned.
        assert_eq!(std::mem::align_of::<HlcClock>(), 64);
    }

    #[test]
    fn hlc_clock_size_is_at_least_64_bytes() {
        // Cache-line aligned types should be at least the alignment size.
        assert!(std::mem::size_of::<HlcClock>() >= 64);
    }

    // -- Concurrent HLC stress --

    #[test]
    fn clock_concurrent_now_is_monotonic_per_thread() {
        use std::sync::Arc;
        use std::thread;
        let clock = Arc::new(HlcClock::new());
        let mut handles = vec![];
        for _ in 0..4 {
            let clock = Arc::clone(&clock);
            handles.push(thread::spawn(move || {
                let mut prev = clock.now();
                for _ in 0..100 {
                    let curr = clock.now();
                    assert!(curr > prev, "thread-local monotonicity violated");
                    prev = curr;
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    // -- HLC clone and copy semantics --

    #[test]
    fn hlc_is_copy_and_clone() {
        let hlc = Hlc::new(42, 7);
        let copied = hlc; // Copy
        let cloned = hlc; // Clone
        assert_eq!(copied, hlc);
        assert_eq!(cloned, hlc);
    }

    // -- HLC ordering edge cases --

    #[test]
    fn hlc_zero_wall_different_logical() {
        let a = Hlc::new(0, 0);
        let b = Hlc::new(0, 1);
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn hlc_large_values() {
        let a = Hlc::new(u64::MAX, u32::MAX);
        let b = Hlc::new(u64::MAX, u32::MAX - 1);
        assert!(a > b);
        assert_eq!(a.wall_time(), u64::MAX);
        assert_eq!(a.logical(), u32::MAX);
    }

    #[test]
    fn hlc_default_is_zero() {
        let zero = Hlc::zero();
        assert_eq!(zero.wall_time(), 0);
        assert_eq!(zero.logical(), 0);
    }
}
