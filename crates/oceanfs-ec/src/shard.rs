//! Zero-copy shard data views via `bytemuck`.
//!
//! Interprets raw byte slices as structured EC shard data without copying.
//! `ShardData` wraps a byte slice and provides access to individual shard
//! elements as `u8` slices through zero-copy casts.

use bytemuck::Pod;
use oceanfs_core::EncodingPlan;

/// A zero-copy view over EC shard data.
///
/// Wraps a byte slice and provides methods to access individual shards
/// without heap allocation. The underlying data is assumed to be laid
/// out in SoA (Struct of Arrays) format — all shards of the same index
/// are stored contiguously.
///
/// # Examples
///
/// ```
/// use oceanfs_ec::ShardData;
/// use oceanfs_core::EncodingPlan;
///
/// let plan = EncodingPlan {
///     stripe_count: 4,
///     padded_size: 1024,
///     shard_size: 64,
///     data_shards: 4,
///     parity_shards: 2,
/// };
/// // 4 data shards × 4 stripes × 64 bytes = 1024 bytes
/// let data = vec![0u8; 1024];
/// let shard_data = ShardData::from_data_shards(&data, 4, &plan);
/// assert_eq!(shard_data.data_shard_count(), 4);
/// assert_eq!(shard_data.parity_shard_count(), 0);
/// assert_eq!(shard_data.shard_size(), 64);
/// ```
pub struct ShardData<'a> {
    /// Raw bytes of the entire segment (all data shards, concatenated).
    raw: &'a [u8],
    /// Number of data shards (k).
    k: u8,
    /// Number of parity shards (m).
    m: u8,
    /// Encoding plan (stripe count, shard size).
    plan: &'a EncodingPlan,
}

impl<'a> ShardData<'a> {
    /// Creates a new `ShardData` view over raw segment data.
    ///
    /// The data is interpreted as k + m shards, each of size
    /// `plan.stripe_count * plan.shard_size`, in SoA layout.
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `raw.len()` does not match the expected
    /// size for `k` data shards plus `m` parity shards.
    pub fn new(raw: &'a [u8], k: u8, m: u8, plan: &'a EncodingPlan) -> Self {
        let expected = (k as usize + m as usize) * plan.stripe_count * plan.shard_size;
        debug_assert_eq!(
            raw.len(),
            expected,
            "shard data length {} does not match expected {}",
            raw.len(),
            expected
        );
        Self { raw, k, m, plan }
    }

    /// Creates a `ShardData` from just data shards (no parity).
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `raw.len()` does not match the expected
    /// size for `k` data shards.
    pub fn from_data_shards(raw: &'a [u8], k: u8, plan: &'a EncodingPlan) -> Self {
        let expected = k as usize * plan.stripe_count * plan.shard_size;
        debug_assert_eq!(
            raw.len(),
            expected,
            "data shard length {} does not match expected {}",
            raw.len(),
            expected
        );
        Self { raw, k, m: 0, plan }
    }

    /// Returns the number of data shards (k).
    pub fn data_shard_count(&self) -> u8 {
        self.k
    }

    /// Returns the number of parity shards (m).
    pub fn parity_shard_count(&self) -> u8 {
        self.m
    }

    /// Returns the size of each shard in bytes.
    pub fn shard_size(&self) -> usize {
        self.plan.shard_size
    }

    /// Returns the shard at the given index as a byte slice.
    ///
    /// Shard indices 0..k are data shards; k..k+m are parity shards.
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `index >= k + m`.
    pub fn shard(&self, index: u8) -> &'a [u8] {
        let idx = index as usize;
        let total = self.k as usize + self.m as usize;
        debug_assert!(idx < total, "shard index {index} out of bounds (max {})", total - 1);
        let shard_len = self.plan.stripe_count * self.plan.shard_size;
        let offset = idx * shard_len;
        &self.raw[offset..offset + shard_len]
    }

    /// Returns the data shard at the given index (0..k).
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `index >= k`.
    pub fn data_shard(&self, index: u8) -> &'a [u8] {
        debug_assert!(
            index < self.k,
            "data shard index {index} out of bounds (max {})",
            self.k - 1
        );
        self.shard(index)
    }

    /// Returns the parity shard at the given index (0..m).
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `index >= m`.
    pub fn parity_shard(&self, index: u8) -> &'a [u8] {
        debug_assert!(
            index < self.m,
            "parity shard index {index} out of bounds (max {})",
            self.m.wrapping_sub(1)
        );
        self.shard(self.k + index)
    }

    /// Returns a slice of a specific stripe within a shard.
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `stripe_index >= plan.stripe_count`.
    pub fn stripe(&self, shard_index: u8, stripe_index: usize) -> &'a [u8] {
        debug_assert!(
            stripe_index < self.plan.stripe_count,
            "stripe index {stripe_index} out of bounds (max {})",
            self.plan.stripe_count - 1
        );
        let shard = self.shard(shard_index);
        let offset = stripe_index * self.plan.shard_size;
        &shard[offset..offset + self.plan.shard_size]
    }

    /// Returns the total number of stripes in each shard.
    pub fn stripe_count(&self) -> usize {
        self.plan.stripe_count
    }

    /// Returns the underlying raw bytes.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.raw
    }
}

/// Marker trait for types that can be safely used with `bytemuck` casts.
///
/// This is automatically implemented for types that are `Pod` + `Send + Sync`.
pub trait ShardPod: Pod + Send + Sync {}

impl<T: Pod + Send + Sync> ShardPod for T {}

/// Reinterprets a byte slice as a slice of `T` using `bytemuck::cast_slice`.
///
/// # Examples
///
/// ```
/// use oceanfs_ec::cast_shard_slice;
///
/// let bytes = [0x01u8, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00];
/// let values: &[u32] = cast_shard_slice(&bytes);
/// assert_eq!(values, &[1u32, 2u32]);
/// ```
pub fn cast_shard_slice<T: Pod>(bytes: &[u8]) -> &[T] {
    bytemuck::cast_slice(bytes)
}

/// Reinterprets a mutable byte slice as a mutable slice of `T`.
pub fn cast_shard_slice_mut<T: Pod>(bytes: &mut [u8]) -> &mut [T] {
    bytemuck::cast_slice_mut(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_data_access_data_shards() {
        let plan = EncodingPlan {
            stripe_count: 2,
            padded_size: 512,
            shard_size: 64,
            data_shards: 4,
            parity_shards: 0,
        };
        // 4 data shards × 2 stripes × 64 bytes = 512 bytes
        let mut raw = vec![0u8; 512];
        // Fill: shard 0 with 0xAA, shard 1 with 0xAB, shard 2 with 0xAC, shard 3 with 0xAD
        for i in 0..4usize {
            let offset = i * 128;
            raw[offset..offset + 128].fill(0xAA + i as u8);
        }

        let sd = ShardData::from_data_shards(&raw, 4, &plan);
        assert_eq!(sd.data_shard_count(), 4);
        assert_eq!(sd.parity_shard_count(), 0);
        assert_eq!(sd.shard_size(), 64);
        assert_eq!(sd.stripe_count(), 2);

        // Check shard 1
        let shard1 = sd.data_shard(1);
        assert_eq!(shard1.len(), 128);
        // Shard 1 should be filled with 0xAB (0xAA + 1)
        assert!(shard1.iter().all(|&b| b == 0xAB));

        // Check stripe 0 of shard 2
        let stripe = sd.stripe(2, 0);
        assert_eq!(stripe.len(), 64);
        // Shard 2 should be filled with 0xAC (0xAA + 2)
        assert!(stripe.iter().all(|&b| b == 0xAC));
    }

    #[test]
    fn shard_data_with_parity() {
        let plan = EncodingPlan {
            stripe_count: 1,
            padded_size: 256,
            shard_size: 64,
            data_shards: 4,
            parity_shards: 2,
        };
        // 4 data + 2 parity × 1 stripe × 64 bytes = 384 bytes
        let raw = vec![0u8; 384];
        let sd = ShardData::new(&raw, 4, 2, &plan);

        assert_eq!(sd.data_shard_count(), 4);
        assert_eq!(sd.parity_shard_count(), 2);

        // Parity shard 0 is at index 4
        let parity0 = sd.parity_shard(0);
        assert_eq!(parity0.len(), 64);

        // Parity shard 1 is at index 5
        let parity1 = sd.parity_shard(1);
        assert_eq!(parity1.len(), 64);
    }

    #[test]
    fn cast_shard_slice_u32() {
        let bytes = vec![0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00];
        let values: &[u32] = cast_shard_slice(&bytes);
        assert_eq!(values, &[1u32, 2u32]);
    }

    #[test]
    fn cast_shard_slice_mut_writes_through() {
        let mut bytes = vec![0u8; 8];
        let values: &mut [u32] = cast_shard_slice_mut(&mut bytes);
        values[0] = 42;
        values[1] = 99;
        let read_back: &[u32] = cast_shard_slice(&bytes);
        assert_eq!(read_back, &[42u32, 99u32]);
    }

    #[test]
    fn shard_data_as_bytes_returns_raw() {
        let plan = EncodingPlan {
            stripe_count: 1,
            padded_size: 256,
            shard_size: 64,
            data_shards: 4,
            parity_shards: 0,
        };
        let raw = vec![0xAAu8; 256];
        let sd = ShardData::from_data_shards(&raw, 4, &plan);
        assert_eq!(sd.as_bytes(), &raw[..]);
        assert_eq!(sd.as_bytes().len(), 256);
    }
}
