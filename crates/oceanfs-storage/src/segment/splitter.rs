//! Segment splitter — splits large blobs into fixed-size chunks.
//!
//! Used when a blob exceeds [`SegmentSizeConfig::default_target_size`].
//! Each chunk becomes a separate segment.

/// Splits blob data into chunk-sized pieces for multi-segment storage.
#[allow(dead_code)]
pub(crate) struct SegmentSplitter {
    chunk_size: u64,
}

#[allow(dead_code)]
impl SegmentSplitter {
    /// Creates a new splitter with the given chunk size.
    pub(crate) fn new(chunk_size: u64) -> Self {
        Self { chunk_size }
    }

    /// Splits data into chunks of at most `chunk_size` bytes.
    ///
    /// Returns `(segment_offset, chunk_data)` pairs. The offset is the
    /// byte position within the original blob.
    pub(crate) fn split<'a>(&self, data: &'a [u8]) -> Vec<(u64, &'a [u8])> {
        let chunk_count = (data.len() as u64).div_ceil(self.chunk_size) as usize;
        let mut chunks = Vec::with_capacity(chunk_count);

        let mut offset: u64 = 0;
        for chunk in data.chunks(self.chunk_size as usize) {
            chunks.push((offset, chunk));
            offset += chunk.len() as u64;
        }

        chunks
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn split_exact_chunks() {
        let splitter = SegmentSplitter::new(10);
        let chunks = splitter.split(&[1u8; 30]);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], (0, &[1u8; 10] as &[u8]));
        assert_eq!(chunks[1], (10, &[1u8; 10] as &[u8]));
        assert_eq!(chunks[2], (20, &[1u8; 10] as &[u8]));
    }

    #[test]
    fn split_uneven_last_chunk() {
        let splitter = SegmentSplitter::new(10);
        let chunks = splitter.split(&[1u8; 25]);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[2].1.len(), 5);
    }

    #[test]
    fn split_smaller_than_chunk() {
        let splitter = SegmentSplitter::new(100);
        let chunks = splitter.split(&[1u8; 5]);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1.len(), 5);
        assert_eq!(chunks[0].0, 0);
    }

    #[test]
    fn split_empty_returns_empty() {
        let splitter = SegmentSplitter::new(10);
        assert!(splitter.split(&[]).is_empty());
    }
}
