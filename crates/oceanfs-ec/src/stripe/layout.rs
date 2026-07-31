//! Stripe layout computation.
//!
//! Computes how many stripes a segment produces given its size and
//! EC parameters (k, m, strip_size). Validates inputs and returns
//! a plan with explicit padding information.

use crate::error::{Error, Result};
use oceanfs_core::EncodingPlan;

/// Computes stripe layout for a segment.
///
/// # Examples
///
/// ```
/// use oceanfs_ec::StripeLayout;
///
/// let plan = StripeLayout::compute(1_048_576, 4, 2, 65536).unwrap();
/// assert_eq!(plan.stripe_count, 4);
/// assert_eq!(plan.shard_size, 65536);
/// ```
pub struct StripeLayout;

impl StripeLayout {
    /// Computes the encoding plan for a segment.
    ///
    /// A segment is split into stripes of `k × strip_size` bytes.
    /// The final stripe is zero-padded if the segment size is not
    /// a multiple of the stripe data size.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidConfig` if:
    /// - `k` is 0
    /// - `strip_size` is 0
    /// - `segment_size` is 0
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_ec::StripeLayout;
    ///
    /// let plan = StripeLayout::compute(1_048_576, 4, 2, 65536).unwrap();
    /// assert_eq!(plan.stripe_count, 4);
    /// assert_eq!(plan.padded_size, 1_048_576);
    /// ```
    pub fn compute(segment_size: u64, k: u8, m: u8, strip_size: usize) -> Result<EncodingPlan> {
        if k == 0 {
            return Err(Error::InvalidConfig("k must be > 0".into()));
        }
        if strip_size == 0 {
            return Err(Error::InvalidConfig("strip_size must be > 0".into()));
        }
        if segment_size == 0 {
            return Err(Error::InvalidConfig("segment_size must be > 0".into()));
        }

        let k64 = k as u64;
        let stripe_data_size = k64 * strip_size as u64;
        let stripe_count = div_ceil_u64(segment_size, stripe_data_size) as usize;
        let padded_size = stripe_count as u64 * stripe_data_size;

        Ok(EncodingPlan {
            stripe_count,
            padded_size,
            shard_size: strip_size,
            data_shards: k,
            parity_shards: m,
        })
    }
}

/// Computes `a / b` rounding up for u64.
///
/// Returns `(a + b - 1) / b`, avoiding overflow by checking the
/// addition first.
pub(crate) fn div_ceil_u64(a: u64, b: u64) -> u64 {
    debug_assert!(b > 0, "divisor must be > 0");
    a.div_ceil(b)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn exact_fit() {
        // 4 * 64KB * 4 stripes = 1 MB
        let plan = StripeLayout::compute(1_048_576, 4, 2, 65536).unwrap();
        assert_eq!(plan.stripe_count, 4);
        assert_eq!(plan.padded_size, 1_048_576);
    }

    #[test]
    fn needs_padding() {
        // 1 MB + 1 byte
        let plan = StripeLayout::compute(1_048_577, 4, 2, 65536).unwrap();
        assert_eq!(plan.stripe_count, 5);
        // 4 * 64KB * 5 = 1_310_720
        assert_eq!(plan.padded_size, 5 * 4 * 65536);
    }

    #[test]
    fn single_stripe() {
        // Exactly one stripe of k * strip_size
        let plan = StripeLayout::compute(4 * 1024, 4, 2, 1024).unwrap();
        assert_eq!(plan.stripe_count, 1);
        assert_eq!(plan.padded_size, 4096);
    }

    #[test]
    fn small_data_partial_stripe() {
        // 100 bytes with k=4, strip_size=64 (stripe data = 256 bytes)
        let plan = StripeLayout::compute(100, 4, 2, 64).unwrap();
        assert_eq!(plan.stripe_count, 1);
        assert_eq!(plan.padded_size, 256);
    }

    #[test]
    fn empty_segment_returns_error() {
        let result = StripeLayout::compute(0, 4, 2, 65536);
        assert!(result.is_err());
    }

    #[test]
    fn zero_k_returns_error() {
        let result = StripeLayout::compute(1024, 0, 2, 64);
        assert!(result.is_err());
    }

    #[test]
    fn zero_strip_size_returns_error() {
        let result = StripeLayout::compute(1024, 4, 2, 0);
        assert!(result.is_err());
    }

    #[test]
    fn padding_roundtrip() {
        // Data that doesn't fill the stripe — plan should indicate padding.
        let plan = StripeLayout::compute(500, 4, 2, 64).unwrap();
        assert_eq!(plan.stripe_count, 2); // 2 * 4 * 64 = 512
        assert_eq!(plan.padded_size, 512);
        assert_eq!(plan.shard_size, 64);
    }

    #[test]
    fn div_ceil_exact() {
        assert_eq!(div_ceil_u64(100, 10), 10);
    }

    #[test]
    fn div_ceil_remainder() {
        assert_eq!(div_ceil_u64(101, 10), 11);
    }

    #[test]
    fn div_ceil_one() {
        assert_eq!(div_ceil_u64(5, 100), 1);
    }
}
