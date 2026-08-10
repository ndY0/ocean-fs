//! The `Hasher` trait and `Blake3Hasher` implementation.
//!
//! Provides a streaming hash interface that avoids buffering the full
//! blob in memory (perf rule 5.2). The `Blake3Hasher` is a thin wrapper
//! around the upstream `blake3::Hasher`, which handles runtime SIMD
//! feature detection (AVX-512, AVX2, SSE4.1, NEON).

use oceanfs_core::HashOutput;

/// A streaming content hasher.
///
/// Feeds data incrementally via [`Hasher::update`] and produces a final hash
/// via [`Hasher::finalize`]. Implementations must be `Send + Sync` so the
/// hasher can be shared across threads for parallel hash verification.
///
/// # Examples
///
/// ```
/// use oceanfs_hash::{Blake3Hasher, Hasher};
///
/// let mut hasher = Blake3Hasher::new();
/// hasher.update(b"hello ");
/// hasher.update(b"world");
/// let hash = hasher.finalize();
/// assert_eq!(hash.as_bytes().len(), 32);
/// ```
pub trait Hasher: Send + Sync {
    /// Feeds additional data into the hasher.
    fn update(&mut self, data: &[u8]);

    /// Finalizes the hash and returns the result.
    ///
    /// This method takes `&self` because the upstream `blake3::Hasher::finalize`
    /// is non-mutating — it copies the internal state.
    fn finalize(&self) -> HashOutput;
}

/// A BLAKE3 streaming hasher.
///
/// Wraps the upstream [`blake3::Hasher`] with runtime SIMD detection.
/// The same binary runs optimally on all targets (AVX-512, AVX2, SSE4.1,
/// NEON, or portable C fallback).
///
/// # Examples
///
/// ```
/// use oceanfs_hash::{Blake3Hasher, Hasher};
///
/// let mut hasher = Blake3Hasher::new();
/// hasher.update(b"some data");
/// let hash = hasher.finalize();
/// ```
#[derive(Clone)]
pub struct Blake3Hasher {
    inner: blake3::Hasher,
}

impl Blake3Hasher {
    /// Creates a new `Blake3Hasher`.
    pub fn new() -> Self {
        Self { inner: blake3::Hasher::new() }
    }

    /// Convenience method: hashes `data` in a single call.
    ///
    /// Equivalent to creating a new hasher, calling [`update`](Hasher::update),
    /// and then [`finalize`](Hasher::finalize).
    pub fn hash(data: &[u8]) -> HashOutput {
        let mut hasher = Self::new();
        hasher.update(data);
        hasher.finalize()
    }
}

impl Default for Blake3Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for Blake3Hasher {
    fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    fn finalize(&self) -> HashOutput {
        let hash = self.inner.finalize();
        HashOutput::from_bytes(*hash.as_bytes())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn blake3_hasher_empty_input_returns_known_hash() {
        let hasher = Blake3Hasher::new();
        let hash = hasher.finalize();
        // BLAKE3 hash of empty input
        let expected = blake3::hash(b"").as_bytes().to_owned();
        assert_eq!(hash.as_bytes(), &expected);
    }

    #[test]
    fn blake3_hasher_update_then_finalize_produces_deterministic_hash() {
        let mut h1 = Blake3Hasher::new();
        h1.update(b"hello world");
        let hash1 = h1.finalize();

        let mut h2 = Blake3Hasher::new();
        h2.update(b"hello ");
        h2.update(b"world");
        let hash2 = h2.finalize();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn blake3_hasher_different_data_produces_different_hash() {
        let mut h1 = Blake3Hasher::new();
        h1.update(b"data1");
        let hash1 = h1.finalize();

        let mut h2 = Blake3Hasher::new();
        h2.update(b"data2");
        let hash2 = h2.finalize();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn blake3_hasher_finalize_is_idempotent() {
        let mut hasher = Blake3Hasher::new();
        hasher.update(b"test");
        let hash1 = hasher.finalize();
        let hash2 = hasher.finalize();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn blake3_hasher_clone_produces_same_hash() {
        let mut h1 = Blake3Hasher::new();
        h1.update(b"clone test");
        let h2 = h1.clone();
        assert_eq!(h1.finalize(), h2.finalize());
    }

    #[test]
    fn blake3_hasher_default_is_empty() {
        let h1 = Blake3Hasher::new();
        let h2 = Blake3Hasher::default();
        assert_eq!(h1.finalize(), h2.finalize());
    }

    #[test]
    fn blake3_hasher_roundtrip_known_string() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let mut hasher = Blake3Hasher::new();
        hasher.update(data);
        let hash = hasher.finalize();
        let expected = blake3::hash(data).as_bytes().to_owned();
        assert_eq!(hash.as_bytes(), &expected);
    }

    #[test]
    fn blake3_hasher_large_input() {
        let data = vec![0xABu8; 1_000_000];
        let mut hasher = Blake3Hasher::new();
        hasher.update(&data);
        let hash = hasher.finalize();
        let expected = blake3::hash(&data).as_bytes().to_owned();
        assert_eq!(hash.as_bytes(), &expected);
    }
}
