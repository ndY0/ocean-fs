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
//! - [`HlcClock`]: thread-safe HLC generator packing the full 96-bit
//!   state (u64 wall ms in the high bits, u32 logical in the low bits)
//!   into a single cache-line-aligned 128-bit atomic, so
//!   `(wall, logical)` always advance atomically under concurrent
//!   access. See
//!   `docs/features/gap-closure/hlc-causality-closure/feature.md` §"Design
//!   Decision: HlcClock State Layout".
//!
//! The atomic is [`portable_atomic::AtomicU128`]: `std` does not ship
//! `AtomicU128` on the workspace toolchain, and the lock-free design
//! (perf guideline §11.1) rules out a mutex fallback. On x86-64 this
//! compiles to native `cmpxchg16b`.

// `AtomicU128` and its `Ordering` come from `portable-atomic` (same
// API as `std::sync::atomic`, which lacks 128-bit atomics).
use portable_atomic::{AtomicU128, Ordering};

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
        self.wall_time.cmp(&other.wall_time).then_with(|| self.logical.cmp(&other.logical))
    }
}

/// A thread-safe Hybrid Logical Clock generator.
///
/// The 96-bit HLC state (u64 wall ms + u32 logical) is packed into a
/// single cache-line-aligned `AtomicU128` and advanced with a CAS loop,
/// so the `(wall, logical)` pair updates atomically — the logical
/// counter can never move backward under concurrent `now()`/`update()`
/// calls. Every call yields a timestamp strictly greater than any
/// previously returned one, even across threads.
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
/// assert!(t1 < t2, "HLC must be monotonically increasing");
/// ```
#[repr(align(64))]
pub struct HlcClock {
    /// Packed state: wall time (u64, milliseconds since epoch) in the
    /// high 32 bits, logical counter (u32) in the low 32 bits.
    state: AtomicU128,
}

impl HlcClock {
    /// Creates a new HLC clock initialized to the current system time.
    ///
    /// The logical counter starts at 0.
    pub fn new() -> Self {
        let wall = current_time_millis() as u128;
        Self { state: AtomicU128::new(wall << 32) }
    }

    /// Returns the current HLC timestamp, advancing the logical counter.
    ///
    /// Implements the HLC local-event rule:
    /// `l.w = max(l.w, pt.now())`, `l.c = l.c + 1` — the wall time is
    /// refreshed from the OS clock on every call and never goes
    /// backward.
    ///
    /// If the logical counter would overflow ([`u32::MAX`] events in a
    /// single millisecond — practically unreachable, but correctness
    /// requires it), the wall time is bumped instead of wrapping.
    ///
    /// This method is lock-free and safe to call from multiple threads
    /// concurrently. Every call returns a timestamp strictly greater
    /// than any previously returned one.
    pub fn now(&self) -> Hlc {
        let physical = current_time_millis();
        loop {
            let cur = self.state.load(Ordering::Acquire);
            let wall = (cur >> 32) as u64;
            let logical = cur as u32;
            let new_wall = wall.max(physical);
            // Overflow guard: wrapping_add would make the logical
            // counter move backward; bump the wall instead.
            let new_logical = logical.wrapping_add(1);
            let (w, l) = if new_logical < logical {
                (new_wall.saturating_add(1), 0u32)
            } else {
                (new_wall, new_logical)
            };
            let next = ((w as u128) << 32) | l as u128;
            if self
                .state
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Hlc { wall_time: w, logical: l };
            }
        }
    }

    /// Updates the HLC clock by merging a received timestamp.
    ///
    /// The HLC receive rule:
    /// 1. `wall = max(local_wall, received.wall_time, pt.now())`
    ///    (the local wall also never lags the OS clock)
    /// 2. If `received.wall_time > local_wall`:
    ///    `logical = received.logical + 1`
    /// 3. Otherwise:
    ///    `logical = max(local_logical, received.logical) + 1`
    ///
    /// This ensures causal consistency: if event A happened-before
    /// event B, then `clock.update(b_hlc) > a_hlc`. Even a *stale*
    /// received timestamp advances the local counter, so every call
    /// yields a fresh, strictly greater local timestamp.
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
        let physical = current_time_millis();
        loop {
            let cur = self.state.load(Ordering::Acquire);
            let wall = (cur >> 32) as u64;
            let logical = cur as u32;
            let new_wall = wall.max(received.wall_time).max(physical);
            let new_logical = if received.wall_time > wall {
                (received.logical as u64).wrapping_add(1)
            } else {
                (logical as u64).max(received.logical as u64).wrapping_add(1)
            };
            // Cap at u32::MAX; bump the wall on overflow (same guard
            // as `now`).
            let (w, l) = if new_logical > u32::MAX as u64 {
                (new_wall.saturating_add(1), 0u32)
            } else {
                (new_wall, new_logical as u32)
            };
            let next = ((w as u128) << 32) | l as u128;
            if self
                .state
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Hlc { wall_time: w, logical: l };
            }
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_types)]
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

    #[test]
    fn clock_wall_tracks_physical_time_after_sleep() {
        use std::time::Duration;

        let clock = HlcClock::new();
        // Physical time captured right after construction. A frozen
        // wall (the old behavior: wall = boot time forever) could
        // never satisfy this comparison once real time advances.
        let physical_at_start = current_time_millis();
        std::thread::sleep(Duration::from_millis(10));
        let ts = clock.now();
        assert!(
            ts.wall_time() >= physical_at_start,
            "wall {} must be >= physical time at construction {}",
            ts.wall_time(),
            physical_at_start,
        );
        // Guard against clock granularity / NTP adjustments: the wall
        // must at least be within 1 s of the current physical time.
        let physical_now = current_time_millis();
        assert!(
            ts.wall_time() >= physical_now.saturating_sub(1000),
            "wall {} must be within 1 s of physical time {}",
            ts.wall_time(),
            physical_now,
        );
    }

    #[test]
    fn clock_now_refreshes_wall_repeatedly() {
        use std::time::Duration;

        let clock = HlcClock::new();
        let first = clock.now();
        std::thread::sleep(Duration::from_millis(10));
        let second = clock.now();
        // Either the wall advanced (physical time merged in) or the
        // logical counter advanced. A regression to the frozen wall
        // would show as an identical wall with a reset logical counter.
        assert!(
            second.wall_time() > first.wall_time() || second.logical() > first.logical(),
            "wall must advance with physical time: first={first:?}, second={second:?}",
        );
    }

    // -- HlcClock receive-merge invariants (hlc-causality-closure G1) --

    #[test]
    fn clock_wall_never_goes_backward_under_update() {
        let clock = HlcClock::new();
        let before = clock.now();
        // Merge an ancient remote timestamp — the local clock must
        // still advance.
        let merged = clock.update(Hlc::new(1, 0));
        assert!(merged > before, "merged {merged:?} must be > before {before:?}");
        let after = clock.now();
        assert!(after > merged, "now() {after:?} must be > merged {merged:?}");
    }

    #[test]
    fn clock_update_merges_remote_wall() {
        let clock = HlcClock::new();
        let now = clock.now();
        let remote = Hlc::new(now.wall_time() + 10_000, 42);
        let merged = clock.update(remote);
        assert!(
            merged.wall_time() >= remote.wall_time(),
            "merged wall {} must reach remote wall {}",
            merged.wall_time(),
            remote.wall_time(),
        );
        let next = clock.now();
        assert!(
            next.wall_time() >= remote.wall_time(),
            "next now() wall {} must stay >= remote wall {}",
            next.wall_time(),
            remote.wall_time(),
        );
    }

    #[test]
    fn clock_update_equal_wall_bumps_logical_past_remote() {
        let clock = HlcClock::new();
        let _first = clock.now(); // logical = 1
        let second = clock.now(); // logical = 2
        let wall = second.wall_time();
        // Receive a timestamp at the *same* wall with a higher logical.
        let updated = clock.update(Hlc::new(wall, 5));
        assert_eq!(
            updated.logical(),
            6,
            "receive rule must bump past the remote logical: {updated:?}",
        );
        assert!(
            updated.wall_time() >= wall,
            "wall must not go backward: {updated:?} vs wall {wall}",
        );
        let after = clock.now();
        assert!(after > updated, "next now() {after:?} must exceed {updated:?}");
    }

    // -- HlcClock concurrency (hlc-causality-closure G1) --

    #[test]
    fn clock_concurrent_now_all_unique() {
        use std::{sync::Arc, thread};

        const THREADS: usize = 8;
        const PER_THREAD: usize = 50_000;

        let clock = Arc::new(HlcClock::new());
        let collected = Arc::new(std::sync::Mutex::new(Vec::with_capacity(THREADS * PER_THREAD)));

        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let clock = Arc::clone(&clock);
            let collected = Arc::clone(&collected);
            handles.push(thread::spawn(move || {
                let mut prev = clock.now();
                let mut local = Vec::with_capacity(PER_THREAD);
                local.push(prev);
                for _ in 1..PER_THREAD {
                    let curr = clock.now();
                    assert!(curr > prev, "per-thread monotonicity violated: {prev:?} vs {curr:?}");
                    prev = curr;
                    local.push(curr);
                }
                collected.lock().unwrap().extend(local);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut all = collected.lock().unwrap();
        let len_before = all.len();
        all.sort();
        all.dedup();
        assert_eq!(
            all.len(),
            len_before,
            "all {} timestamps must be distinct across threads ({} dupes)",
            len_before,
            len_before - all.len(),
        );
        assert_eq!(len_before, THREADS * PER_THREAD);
    }

    #[test]
    fn clock_concurrent_update_and_now_never_duplicate() {
        use std::sync::Arc;

        const NOW_THREADS: usize = 4;
        const UPDATE_THREADS: usize = 2;
        const ITERATIONS: usize = 100_000;

        let clock = Arc::new(HlcClock::new());
        let collected = Arc::new(std::sync::Mutex::new(Vec::with_capacity(
            (NOW_THREADS + UPDATE_THREADS) * ITERATIONS,
        )));

        let mut handles = Vec::with_capacity(NOW_THREADS + UPDATE_THREADS);
        for _ in 0..NOW_THREADS {
            let clock = Arc::clone(&clock);
            let collected = Arc::clone(&collected);
            handles.push(std::thread::spawn(move || {
                let mut prev = clock.now();
                for _ in 0..ITERATIONS {
                    let curr = clock.now();
                    assert!(curr > prev, "per-thread monotonicity violated");
                    prev = curr;
                    collected.lock().unwrap().push(curr);
                }
            }));
        }
        for t in 0..UPDATE_THREADS {
            let clock = Arc::clone(&clock);
            let collected = Arc::clone(&collected);
            handles.push(std::thread::spawn(move || {
                let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ (t as u64 + 1);
                let mut prev = Hlc::new(0, 0);
                for _ in 0..ITERATIONS {
                    // xorshift64 — deterministic per-thread random HLCs.
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let remote = Hlc::new(state % 1_000_000, (state >> 32) as u32 % 1000);
                    let curr = clock.update(remote);
                    assert!(
                        curr > prev,
                        "per-thread monotonicity violated after update({remote:?})",
                    );
                    prev = curr;
                    collected.lock().unwrap().push(curr);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut all = collected.lock().unwrap();
        let len_before = all.len();
        all.sort();
        all.dedup();
        assert_eq!(
            all.len(),
            len_before,
            "all {} timestamps must be distinct ({} dupes)",
            len_before,
            len_before - all.len(),
        );
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
        use std::{sync::Arc, thread};
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

    #[test]
    fn hlc_clock_default_is_new() {
        let clock = HlcClock::default();
        let ts = clock.now();
        assert!(ts.wall_time() > 0);
    }
}
