//! Multi-chunk assembler with streaming BLAKE3 verification.
//!
//! Accumulates chunk data from multiple segment reads in order
//! and verifies the combined BLAKE3 hash against the stored
//! hash once all chunks are received.
//!
//! Per performance guideline §5.2 (streaming BLAKE3 — never buffer
//! the full blob before hashing) and §5.4 (single hasher for
//! multi-chunk reads).

use bytes::{Bytes, BytesMut};
use oceanfs_core::HashOutput;

use crate::error::{Error, Result};

/// Assembles blob data from multiple chunks, verifying the
/// BLAKE3 hash on completion.
///
/// Chunks must be pushed in the correct order (by their index in
/// the chunk list). The assembler feeds each chunk through a
/// streaming [`blake3::Hasher`] and verifies the final hash
/// against the expected hash when [`finalize`](Self::finalize)
/// is called.
///
/// # Examples
///
/// ```
/// # use oceanfs_core::HashOutput;
/// # use oceanfs_server::MultiChunkAssembler;
/// # use bytes::Bytes;
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let expected = blake3::hash(b"hello world");
/// let expected_hash = HashOutput::from_bytes(*expected.as_bytes());
///
/// let mut assembler = MultiChunkAssembler::new(expected_hash, 2);
/// assembler.push_chunk(0, Bytes::from_static(b"hello "))?;
/// assembler.push_chunk(1, Bytes::from_static(b"world"))?;
/// let result = assembler.finalize()?;
/// assert_eq!(&result[..], b"hello world");
/// # Ok(())
/// # }
/// ```
pub struct MultiChunkAssembler {
    /// Streaming BLAKE3 hasher fed chunk-by-chunk.
    hasher: blake3::Hasher,
    /// Expected hash to verify against after all chunks are
    /// received.
    expected_hash: HashOutput,
    /// Accumulated data, built by appending each chunk.
    buffer: BytesMut,
    /// Number of chunks expected.
    chunk_count: usize,
    /// Number of chunks received so far.
    received: usize,
    /// Whether a BLAKE3 hash verification should be performed.
    /// Set to `false` when no hash is stored in metadata.
    verify: bool,
}

impl MultiChunkAssembler {
    /// Creates a new assembler expecting `chunk_count` total chunks.
    ///
    /// The `expected_hash` is the stored BLAKE3 hash of the
    /// complete blob from the object metadata.
    pub fn new(expected_hash: HashOutput, chunk_count: usize) -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            expected_hash,
            buffer: BytesMut::with_capacity(64 * 1024), // pre-allocate 64 KB
            chunk_count,
            received: 0,
            verify: true,
        }
    }

    // [review][implementation][high]
    // why a default 64Mb buffer ?
    // [end]
    /// Creates a new assembler without hash verification.
    ///
    /// Used when the object metadata has no stored BLAKE3 hash.
    pub fn new_no_verify(chunk_count: usize) -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            expected_hash: HashOutput::from_bytes([0u8; 32]),
            buffer: BytesMut::with_capacity(64 * 1024),
            chunk_count,
            received: 0,
            verify: false,
        }
    }
    // [review][implementation][critical]
    // when we will introduce multi-part uploads, client payloads could be Gb in size.
    // we cannot afford to accumulate, we will need a end to end streaming approach.
    // [end]
    /// Pushes a chunk of data into the assembler.
    ///
    /// Chunks must be pushed in order (index `0, 1, 2, ...`).
    /// The data is appended to the internal buffer and fed
    /// through the streaming hasher.
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk index does not match the
    /// expected next index (chunks out of order).
    pub fn push_chunk(&mut self, index: usize, data: Bytes) -> Result<()> {
        if index != self.received {
            return Err(Error::Internal(format!(
                "chunk out of order: expected index {}, got {}",
                self.received, index
            )));
        }

        self.hasher.update(&data);
        self.buffer.extend_from_slice(&data);
        self.received += 1;
        Ok(())
    }

    /// Finalizes the assembly, verifying the BLAKE3 hash.
    ///
    /// Returns the complete blob data as [`Bytes`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if not all expected chunks
    /// were received.
    /// Returns [`Error::HashMismatch`] if the computed hash
    /// does not match the expected hash.
    pub fn finalize(self) -> Result<Bytes> {
        if self.received != self.chunk_count {
            return Err(Error::Internal(format!(
                "incomplete assembly: expected {} chunks, got {}",
                self.chunk_count, self.received
            )));
        }

        if self.verify {
            let computed = self.hasher.finalize();
            if computed.as_bytes() != self.expected_hash.as_bytes() {
                return Err(Error::HashMismatch {
                    expected: hex::encode(self.expected_hash.as_bytes()),
                    actual: hex::encode(computed.as_bytes()),
                });
            }
        }

        Ok(self.buffer.freeze())
    }

    /// Returns the total number of bytes accumulated so far.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if no chunks have been pushed yet.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::HashOutput;

    use super::*;

    #[test]
    fn assembler_single_chunk_hash_matches() {
        let data = b"hello world";
        let hash = blake3::hash(data);
        let expected = HashOutput::from_bytes(*hash.as_bytes());

        let mut assembler = MultiChunkAssembler::new(expected, 1);
        assembler.push_chunk(0, Bytes::from_static(data)).unwrap();
        let result = assembler.finalize().unwrap();
        assert_eq!(&result[..], data);
    }

    #[test]
    fn assembler_multi_chunk_correct_order() {
        let part1 = b"hello ";
        let part2 = b"world";
        let combined: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();
        let hash = blake3::hash(&combined);
        let expected = HashOutput::from_bytes(*hash.as_bytes());

        let mut assembler = MultiChunkAssembler::new(expected, 2);
        assembler.push_chunk(0, Bytes::from_static(part1)).unwrap();
        assembler.push_chunk(1, Bytes::from_static(part2)).unwrap();
        let result = assembler.finalize().unwrap();
        assert_eq!(&result[..], &combined[..]);
    }

    #[test]
    fn assembler_wrong_order_returns_error() {
        let part1 = b"hello ";
        let part2 = b"world";
        let combined: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();
        let hash = blake3::hash(&combined);
        let expected = HashOutput::from_bytes(*hash.as_bytes());

        let mut assembler = MultiChunkAssembler::new(expected, 2);
        let err = assembler.push_chunk(1, Bytes::from_static(part2)).unwrap_err();
        assert!(err.to_string().contains("out of order"), "expected out-of-order error");
    }

    #[test]
    fn assembler_hash_mismatch_returns_error() {
        let data = b"hello world";
        let wrong_hash = blake3::hash(b"different data");
        let expected = HashOutput::from_bytes(*wrong_hash.as_bytes());

        let mut assembler = MultiChunkAssembler::new(expected, 1);
        assembler.push_chunk(0, Bytes::from_static(data)).unwrap();
        let err = assembler.finalize().unwrap_err();
        assert!(matches!(err, Error::HashMismatch { .. }), "expected HashMismatch error");
    }

    #[test]
    fn assembler_empty_chunks_is_ok() {
        let hash = blake3::hash(b"");
        let expected = HashOutput::from_bytes(*hash.as_bytes());

        let assembler = MultiChunkAssembler::new(expected, 0);
        let result = assembler.finalize().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn assembler_no_verify_skips_hash_check() {
        let mut assembler = MultiChunkAssembler::new_no_verify(1);
        assembler.push_chunk(0, Bytes::from_static(b"anything")).unwrap();
        let result = assembler.finalize().unwrap();
        assert_eq!(&result[..], b"anything");
    }

    #[test]
    fn assembler_incomplete_returns_error() {
        let hash = blake3::hash(b"hello world");
        let expected = HashOutput::from_bytes(*hash.as_bytes());

        let mut assembler = MultiChunkAssembler::new(expected, 2);
        assembler.push_chunk(0, Bytes::from_static(b"hello ")).unwrap();
        let err = assembler.finalize().unwrap_err();
        assert!(err.to_string().contains("incomplete"), "expected incomplete-assembly error");
    }
}
