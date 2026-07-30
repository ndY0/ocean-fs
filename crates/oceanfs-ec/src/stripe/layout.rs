//! Stripe layout computation.
//!
//! Computes how many stripes a segment produces given its size and
//! EC parameters (k, m, strip_size).

use oceanfs_core::EncodingPlan;

/// Computes stripe layout for a segment.
pub struct StripeLayout;

impl StripeLayout {
    /// Computes the encoding plan for a segment.
    ///
    /// A segment is split into stripes of `k × strip_size` bytes.
    /// The final stripe is zero-padded if the segment size is not
    /// a multiple of the stripe size.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_ec::StripeLayout;
    ///
    /// let plan = StripeLayout::compute(1_048_576, 4, 2, 65536);
    /// assert_eq!(plan.stripe_count, 4);
    /// assert_eq!(plan.shard_size, 65536);
    /// ```
    pub fn compute(segment_size: u64, k: u8, _m: u8, strip_size: usize) -> EncodingPlan {
        let k = k as u64;
        let stripe_data_size = k * strip_size as u64;
        let stripe_count = segment_size.div_ceil(stripe_data_size) as usize;
        let padded_size = stripe_count as u64 * stripe_data_size;

        EncodingPlan { stripe_count, padded_size, shard_size: strip_size }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn exact_fit() {
        // 4 * 64KB * 4 stripes = 1 MB
        let plan = StripeLayout::compute(1_048_576, 4, 2, 65536);
        assert_eq!(plan.stripe_count, 4);
        assert_eq!(plan.padded_size, 1_048_576);
    }

    #[test]
    fn needs_padding() {
        // 1 MB + 1 byte
        let plan = StripeLayout::compute(1_048_577, 4, 2, 65536);
        assert_eq!(plan.stripe_count, 5);
        assert_eq!(plan.padded_size, 4 * 65536 * 5);
    }

    #[test]
    fn empty_segment() {
        let plan = StripeLayout::compute(0, 4, 2, 65536);
        assert_eq!(plan.stripe_count, 0);
    }
}
