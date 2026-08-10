//! The `BatchHasher` trait and `Blake3BatchHasher` implementation.
//!
//! Hashes multiple chunks independently and returns one hash per chunk.
//! Used for verifying multi-chunk blobs where each chunk's hash must be
//! compared against the stored segment index entry.

use crate::hasher::{Blake3Hasher, Hasher};
use oceanfs_core::HashOutput;

/// A hasher that processes multiple chunks independently.
///
/// Each chunk is hashed separately — not concatenated. This is used
/// when individual chunk hashes are stored in the segment index and
/// must be verified independently.
///
/// # Examples
///
/// ```
/// use oceanfs_hash::{Blake3BatchHasher, BatchHasher};
///
/// let hasher = Blake3BatchHasher::new();
/// let hashes = hasher.hash_chunks(&[b"chunk1", b"chunk2"]);
/// assert_eq!(hashes.len(), 2);
/// ```
pub trait BatchHasher: Send + Sync {
    /// Hashes multiple chunks independently.
    ///
    /// Returns one [`HashOutput`] per chunk in the same order as the input.
    fn hash_chunks(&self, chunks: &[&[u8]]) -> Vec<HashOutput>;
}

/// A BLAKE3 batch hasher.
///
/// Hashes each chunk independently using a fresh [`Blake3Hasher`].
/// Suitable for verifying segment index entries where each blob chunk's
/// hash is stored separately.
///
/// # Examples
///
/// ```
/// use oceanfs_hash::{Blake3BatchHasher, BatchHasher};
///
/// let hasher = Blake3BatchHasher::new();
/// let chunks: &[&[u8]] = &[b"hello", b"world"];
/// let hashes = hasher.hash_chunks(chunks);
/// assert_eq!(hashes.len(), 2);
/// // Each chunk hash matches blake3::hash(chunk)
/// assert_eq!(hashes[0].as_bytes(), blake3::hash(b"hello").as_bytes());
/// assert_eq!(hashes[1].as_bytes(), blake3::hash(b"world").as_bytes());
/// ```
#[derive(Clone, Debug, Default)]
pub struct Blake3BatchHasher;

impl Blake3BatchHasher {
    /// Creates a new `Blake3BatchHasher`.
    pub fn new() -> Self {
        Self
    }
}

impl BatchHasher for Blake3BatchHasher {
    fn hash_chunks(&self, chunks: &[&[u8]]) -> Vec<HashOutput> {
        chunks
            .iter()
            .map(|chunk| {
                let mut hasher = Blake3Hasher::new();
                hasher.update(chunk);
                hasher.finalize()
            })
            .collect()
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
    fn batch_hasher_empty_input_returns_empty_vec() {
        let hasher = Blake3BatchHasher::new();
        let hashes = hasher.hash_chunks(&[]);
        assert!(hashes.is_empty());
    }

    #[test]
    fn batch_hasher_single_chunk_matches_blake3_direct() {
        let hasher = Blake3BatchHasher::new();
        let chunk = b"single chunk";
        let hashes = hasher.hash_chunks(&[chunk]);
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].as_bytes(), blake3::hash(chunk).as_bytes());
    }

    #[test]
    fn batch_hasher_multiple_chunks_independent() {
        let hasher = Blake3BatchHasher::new();
        let hashes = hasher.hash_chunks(&[b"a", b"b", b"c"]);
        assert_eq!(hashes.len(), 3);
        assert_eq!(hashes[0].as_bytes(), blake3::hash(b"a").as_bytes());
        assert_eq!(hashes[1].as_bytes(), blake3::hash(b"b").as_bytes());
        assert_eq!(hashes[2].as_bytes(), blake3::hash(b"c").as_bytes());
    }

    #[test]
    fn batch_hasher_identical_chunks_produce_identical_hashes() {
        let hasher = Blake3BatchHasher::new();
        let hashes = hasher.hash_chunks(&[b"same", b"same"]);
        assert_eq!(hashes.len(), 2);
        assert_eq!(hashes[0], hashes[1]);
    }

    #[test]
    fn batch_hasher_different_chunks_produce_different_hashes() {
        let hasher = Blake3BatchHasher::new();
        let hashes = hasher.hash_chunks(&[b"one", b"two"]);
        assert_eq!(hashes.len(), 2);
        assert_ne!(hashes[0], hashes[1]);
    }
}
