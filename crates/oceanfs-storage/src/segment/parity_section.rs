//! EC parity section — the on-disk layout shared by the sealer
//! (writer) and the repair path (reader).
//!
//! Sealed segment files (format v2) are laid out as:
//!
//! ```text
//! header + data + [parity section] + index
//! ```
//!
//! The parity section stores the seal-time computed parity shards (the
//! parallel encoder runs on the blocking pool — single scheduler) plus
//! a per-shard BLAKE3 hash table that lets the read path locate a
//! corrupt shard precisely and reconstruct it from the surviving shards.
//!
//! Only COMPLETE stripes are encoded: segments smaller than one stripe
//! (`k × strip`, 256 KiB with the default codec — e.g. the 64 KiB small
//! tier) carry no parity and rely on the replica-fetch recovery path.
//! This is a deliberate coverage change vs. the retired streaming
//! design, which zero-padded and encoded the final partial stripe.
//!
//! ## Layout
//!
//! ```text
//! [u32 stripe_count][u8 k][u8 m][u32 strip_size]    12-byte header
//! [stripe_count × m × strip_size parity bytes]      shards, per stripe
//! [stripe_count × (k+m) × 32 shard hashes]          data shards 0..k,
//!                                                   parity shards k..k+m
//! ```

use bytes::Bytes;

/// Size of the fixed parity-section header.
pub const PARITY_SECTION_HEADER_SIZE: usize = 12;

/// Size of one shard hash in the per-shard hash table.
pub const SHARD_HASH_SIZE: usize = 32;

/// A parsed parity section (borrows the section bytes).
#[derive(Debug)]
pub(crate) struct ParitySection<'a> {
    /// Number of fully-encoded stripes.
    pub stripe_count: usize,
    /// Number of EC data shards per stripe (k).
    pub k: usize,
    /// Number of EC parity shards per stripe (m).
    pub m: usize,
    /// Size of one shard in bytes.
    pub strip: usize,
    /// The parity shard bytes: `stripe_count × m × strip`.
    shards: &'a [u8],
    /// The hash table: `stripe_count × (k+m) × 32`.
    hashes: &'a [u8],
}

impl<'a> ParitySection<'a> {
    /// Parses a parity section. Returns `None` on any malformed input.
    pub(crate) fn parse(section: &'a [u8]) -> Option<Self> {
        if section.len() < PARITY_SECTION_HEADER_SIZE {
            return None;
        }
        let stripe_count = u32::from_le_bytes(section[0..4].try_into().ok()?) as usize;
        let k = section[4] as usize;
        let m = section[5] as usize;
        let strip = u32::from_le_bytes(section[8..12].try_into().ok()?) as usize;
        if stripe_count == 0 || k == 0 || m == 0 || strip == 0 {
            return None;
        }
        let shards_len = stripe_count * m * strip;
        let hashes_len = stripe_count * (k + m) * SHARD_HASH_SIZE;
        if section.len() != PARITY_SECTION_HEADER_SIZE + shards_len + hashes_len {
            return None;
        }
        let shards = &section[PARITY_SECTION_HEADER_SIZE..PARITY_SECTION_HEADER_SIZE + shards_len];
        let hashes = &section[PARITY_SECTION_HEADER_SIZE + shards_len..];
        Some(Self { stripe_count, k, m, strip, shards, hashes })
    }

    /// Byte length of one full stripe of data: `k × strip`.
    pub(crate) fn stripe_len(&self) -> usize {
        self.k * self.strip
    }

    /// The `shard`-th data shard of `stripe` (must be < k).
    pub(crate) fn data_shard<'b>(&self, data: &'b [u8], stripe: usize, shard: usize) -> &'b [u8] {
        let base = stripe * self.stripe_len() + shard * self.strip;
        &data[base..base + self.strip]
    }

    /// The `shard`-th parity shard of `stripe` (must be < m).
    pub(crate) fn parity_shard(&self, stripe: usize, shard: usize) -> &[u8] {
        let base = (stripe * self.m + shard) * self.strip;
        &self.shards[base..base + self.strip]
    }

    /// The expected hash of shard index `idx` (0..k data, k..k+m parity)
    /// of `stripe`.
    pub(crate) fn shard_hash(&self, stripe: usize, idx: usize) -> &[u8; SHARD_HASH_SIZE] {
        let base = (stripe * (self.k + self.m) + idx) * SHARD_HASH_SIZE;
        self.hashes[base..base + SHARD_HASH_SIZE]
            .try_into()
            .unwrap_or_else(|_| panic!("hash table bounds validated at parse"))
    }
}

/// Builds the parity section bytes for a sealed segment.
///
/// `parity` holds `m` shards per completed stripe in
/// `[stripe0_p0, stripe0_p1, ..., stripe1_p0, ...]` order; each shard is
/// `strip_size` bytes. Returns `None` when no usable parity was produced
/// (plain segments, empty segments, or malformed input).
pub(crate) fn build_parity_section(
    data: &[u8],
    ec_k: u8,
    ec_m: u8,
    parity: Option<&[Bytes]>,
) -> Option<Vec<u8>> {
    let parity = parity?;
    let k = ec_k as usize;
    let m = ec_m as usize;
    if parity.is_empty() || k == 0 || m == 0 {
        return None;
    }
    let strip = parity[0].len();
    if strip == 0 || parity.len() % m != 0 {
        return None;
    }
    let stripe_count = parity.len() / m;
    let stripe_byte_len = k * strip;
    // All shards must be uniform and the data must cover the stripes.
    if parity.iter().any(|s| s.len() != strip) || data.len() < stripe_count * stripe_byte_len {
        return None;
    }

    let hash = |bytes: &[u8]| *blake3::hash(bytes).as_bytes();
    let mut out = Vec::with_capacity(
        PARITY_SECTION_HEADER_SIZE
            + parity.len() * strip
            + stripe_count * (k + m) * SHARD_HASH_SIZE,
    );
    out.extend_from_slice(&(stripe_count as u32).to_le_bytes());
    out.push(ec_k);
    out.push(ec_m);
    out.extend_from_slice(&[0u8; 2]); // padding
    out.extend_from_slice(&(strip as u32).to_le_bytes());
    for shard in parity {
        out.extend_from_slice(shard);
    }
    for stripe in 0..stripe_count {
        let base = stripe * stripe_byte_len;
        for d in 0..k {
            out.extend_from_slice(&hash(&data[base + d * strip..base + (d + 1) * strip]));
        }
        for p in 0..m {
            out.extend_from_slice(&hash(&parity[stripe * m + p]));
        }
    }
    Some(out)
}

/// Encodes a segment's complete stripes into the v2 parity-section shard
/// order (`[stripe0_p0, stripe0_p1, ..., stripe1_p0, ...]`, each shard
/// `strip` bytes) using the parallel encoder.
///
/// Only stripes fully covered by `data` are encoded (the segment tail —
/// up to one stripe — is unprotected, matching the section format).
/// Returns `None` when EC is not configured, the segment has no complete
/// stripe, or the encode fails (the seal then persists no parity and
/// degrades to the replica-fetch recovery path).
///
/// The encode is CPU-bound and rayon-backed; callers should run it via
/// `tokio::task::spawn_blocking` (the seal worker does).
#[cfg(test)]
pub(crate) static LAST_ENCODE_THREAD: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(crate) fn encode_segment_parity(data: &[u8], k: u8, m: u8, strip: usize) -> Option<Vec<Bytes>> {
    // Test seam: record the thread this encode ran on, so the sealer
    // test can pin the spawn_blocking boundary (the CPU-bound encode
    // must never run on a tokio worker).
    #[cfg(test)]
    {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        LAST_ENCODE_THREAD.store(hasher.finish(), std::sync::atomic::Ordering::Relaxed);
    }

    if k == 0 || m == 0 || strip == 0 {
        return None;
    }
    let stripe_byte_len = k as usize * strip;
    let complete = data.len() / stripe_byte_len;
    if complete == 0 {
        return None;
    }
    let plan =
        oceanfs_ec::StripeLayout::compute((complete * stripe_byte_len) as u64, k, m, strip).ok()?;
    let encoder = oceanfs_ec::ParallelEncoder::new(
        std::sync::Arc::new(oceanfs_ec::CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: k,
            parity_shards: m,
            strip_size_bytes: strip,
            ..Default::default()
        })),
        0, // the seal worker bounds concurrency via its own semaphore
    );
    let batch = encoder.encode(&data[..complete * stripe_byte_len], &plan).ok()?;

    // SoA (m buffers x complete x strip) → AoS ([stripe][parity] shards).
    let mut shards = Vec::with_capacity(complete * m as usize);
    for stripe in 0..complete {
        for p in 0..m as usize {
            let base = stripe * strip;
            shards.push(Bytes::copy_from_slice(&batch.parity[p][base..base + strip]));
        }
    }
    Some(shards)
}

/// Verifies that a parity section's hash table matches the data and
/// parity shards it claims to cover. Used by tests.
#[cfg(test)]
pub(crate) fn verify_section_hashes(section: &ParitySection, data: &[u8]) -> bool {
    for stripe in 0..section.stripe_count {
        for d in 0..section.k {
            let shard = section.data_shard(data, stripe, d);
            if *blake3::hash(shard).as_bytes() != *section.shard_hash(stripe, d) {
                return false;
            }
        }
        for p in 0..section.m {
            let shard = section.parity_shard(stripe, p);
            if *blake3::hash(shard).as_bytes() != *section.shard_hash(stripe, section.k + p) {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_ec::Encoder;

    use super::*;

    #[test]
    fn build_and_parse_roundtrip() {
        let k = 4u8;
        let m = 2u8;
        let strip = 64usize;
        let data: Vec<u8> = (0..512u32).map(|i| (i % 251) as u8).collect();
        let codec = oceanfs_ec::CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: k,
            parity_shards: m,
            strip_size_bytes: strip,
            ..Default::default()
        });
        let mut parity: Vec<Bytes> = Vec::new();
        for stripe in 0..2 {
            let shards: Vec<&[u8]> = (0..4)
                .map(|d| &data[stripe * 256 + d * strip..stripe * 256 + (d + 1) * strip])
                .collect();
            parity.extend(codec.encode(&shards, m).unwrap());
        }
        let section = build_parity_section(&data, k, m, Some(&parity)).unwrap();
        let parsed = ParitySection::parse(&section).unwrap();
        assert_eq!(parsed.stripe_count, 2);
        assert_eq!(parsed.k, 4);
        assert_eq!(parsed.m, 2);
        assert_eq!(parsed.strip, 64);
        assert!(verify_section_hashes(&parsed, &data), "hash table must match");
    }

    #[test]
    fn build_returns_none_for_plain_or_empty() {
        let data = vec![0u8; 256];
        assert!(build_parity_section(&data, 4, 2, None).is_none());
        assert!(build_parity_section(&data, 4, 2, Some(&[])).is_none());
        assert!(build_parity_section(&data, 0, 0, Some(&[Bytes::from_static(b"x")])).is_none());
    }

    #[test]
    fn parse_rejects_malformed_sections() {
        assert!(ParitySection::parse(&[]).is_none());
        assert!(ParitySection::parse(&[0u8; 12]).is_none(), "zero stripe_count");
    }
}
