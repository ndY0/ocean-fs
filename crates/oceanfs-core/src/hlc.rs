//! Hybrid Logical Clock (HLC).
//!
//! HLC provides causally-consistent total ordering without requiring
//! synchronized physical clocks. Each timestamp combines a wall-clock
//! component (`wall_time`) with a logical counter to disambiguate
//! events at the same physical time.

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
