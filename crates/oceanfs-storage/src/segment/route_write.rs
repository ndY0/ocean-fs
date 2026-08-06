//! Write-path orchestration — routes blob writes through the tier system.
//!
//! Contains `InlineWriter` for inline blob storage and the `route_write`
//! function that dispatches writes to the correct tier.

use bytes::Bytes;
use oceanfs_core::{ChunkRef, ObjectKey, SizeTier};

use crate::{
    error::Result,
    segment::{splitter::SegmentSplitter, tier::TierRouter},
    RocksDbMetadataStore,
};

/// Writes blobs directly to the metadata store (inline path).
// TODO(write-path-unification): remove once cleanup task L5 is done.
#[allow(dead_code)]
pub(crate) struct InlineWriter;

impl InlineWriter {
    /// Stores a blob inline in metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata write fails.
    #[allow(dead_code)]
    pub(crate) fn write_inline(
        metadata: &RocksDbMetadataStore,
        key: ObjectKey,
        data: Bytes,
    ) -> Result<()> {
        let meta = oceanfs_core::ObjectMetadata {
            object_key: key,
            size: data.len() as u64,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(data),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            hlc: oceanfs_core::Hlc::zero(),
        };
        metadata.put_object(meta)
    }
}

/// Routes a blob write to the appropriate tier.
// TODO(write-path-unification): remove once cleanup task L5 is done.
#[allow(dead_code)]
pub(crate) fn route_write(
    router: &TierRouter,
    metadata: &RocksDbMetadataStore,
    active: &mut crate::segment::buffer::ActiveSegment,
    key: ObjectKey,
    data: Bytes,
) -> Result<smallvec::SmallVec<[oceanfs_core::ChunkRef; 4]>> {
    let blob_size = data.len() as u64;
    if blob_size == 0 {
        return Ok(smallvec::SmallVec::new());
    }

    let tier = router.classify(blob_size);

    match tier {
        SizeTier::Inline => {
            InlineWriter::write_inline(metadata, key, data)?;
            Ok(smallvec::SmallVec::new())
        }
        SizeTier::Small | SizeTier::Standard => {
            let segment_id = active.id();
            let (offset, length) = active.append(&data)?;
            let mut chunks = smallvec::SmallVec::new();
            chunks.push(ChunkRef { segment_id, offset, length: length as u32 });
            Ok(chunks)
        }
        SizeTier::Multi => {
            let splitter = SegmentSplitter::new(router.target_size(SizeTier::Multi));
            let chunks = splitter.split(&data);
            let mut chunk_refs = smallvec::SmallVec::with_capacity(chunks.len());
            for (chunk_offset, chunk_data) in &chunks {
                let segment_id = active.id();
                let (_offset, _length) = active.append(chunk_data)?;
                chunk_refs.push(ChunkRef {
                    segment_id,
                    offset: *chunk_offset,
                    length: chunk_data.len() as u32,
                });
            }
            Ok(chunk_refs)
        }
        _ => Err(crate::error::Error::InvalidTier(format!(
            "unsupported storage tier for write routing: {tier:?}"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::SegmentSizeConfig;

    use super::*;
    use crate::buffer_pool::BufferPool;

    fn test_config() -> RocksDbMetadataStore {
        let dir = tempfile::tempdir().unwrap();
        RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
        })
        .unwrap()
    }

    fn test_pool(chunk_size: usize, max: usize) -> BufferPool {
        BufferPool::new(chunk_size, max)
    }

    // ------------------------------------------------------------------
    // Inline path
    // ------------------------------------------------------------------

    #[test]
    fn route_write_inline_stores_in_metadata() {
        let metadata = test_config();
        let router = TierRouter::new(SegmentSizeConfig::default());
        let pool = test_pool(65536, 8);
        let mut active = crate::segment::buffer::ActiveSegment::new(
            SizeTier::Small,
            &SegmentSizeConfig::default(),
            &pool,
        )
        .unwrap();
        let key = ObjectKey::new("inline-test");

        let data = Bytes::from_static(b"tiny"); // 4 bytes ≤ 4096 → Inline
        let refs = route_write(&router, &metadata, &mut active, key.clone(), data.clone()).unwrap();

        assert!(refs.is_empty(), "inline blobs have no chunk refs");
        let fetched = metadata
            .get_object(&oceanfs_core::BucketId::new("default"), &key)
            .unwrap()
            .expect("object not found");
        assert!(fetched.is_inline());
        assert_eq!(fetched.inline_data.as_deref(), Some(&data[..]));
    }

    // ------------------------------------------------------------------
    // Small / Standard path
    // ------------------------------------------------------------------

    #[test]
    fn route_write_small_tier_returns_chunk_ref() {
        let metadata = test_config();
        // Custom config: inline threshold = 0 so 5KB goes to Small tier
        let config =
            SegmentSizeConfig { inline_threshold_bytes: 0, ..SegmentSizeConfig::default() };
        let router = TierRouter::new(config);
        let pool = test_pool(65536, 8);
        let mut active = crate::segment::buffer::ActiveSegment::new(
            SizeTier::Small,
            &SegmentSizeConfig::default(),
            &pool,
        )
        .unwrap();
        let key = ObjectKey::new("small-test");
        let data = Bytes::from(vec![0xCC; 5000]); // 5 KB → Small (≤256KB)

        let refs = route_write(&router, &metadata, &mut active, key, data).unwrap();

        assert_eq!(refs.len(), 1, "small blob should produce one chunk ref");
        assert_eq!(refs[0].offset, 0);
        assert_eq!(refs[0].length, 5000);
    }

    #[test]
    fn route_write_standard_tier_returns_chunk_ref() {
        let metadata = test_config();
        // 1 MB blob → Standard tier
        let router = TierRouter::new(SegmentSizeConfig::default());
        let pool = test_pool(65536, 8);
        let mut active = crate::segment::buffer::ActiveSegment::new(
            SizeTier::Standard,
            &SegmentSizeConfig::default(),
            &pool,
        )
        .unwrap();
        let key = ObjectKey::new("std-test");
        let data = Bytes::from(vec![0xDD; 1_048_576]); // 1 MB → Standard

        let refs = route_write(&router, &metadata, &mut active, key, data).unwrap();

        assert_eq!(refs.len(), 1, "standard blob should produce one chunk ref");
        assert_eq!(refs[0].offset, 0);
        assert_eq!(refs[0].length, 1_048_576);
    }

    // ------------------------------------------------------------------
    // Multi segment path
    // ------------------------------------------------------------------

    #[test]
    fn route_write_multi_tier_splits_into_chunks() {
        let metadata = test_config();
        // Custom config: default_target_size = 1 MB so 3 MB blob is Multi
        let config = SegmentSizeConfig {
            default_target_size: 1_048_576, // 1 MB
            ..SegmentSizeConfig::default()
        };
        let router = TierRouter::new(config);
        let pool = BufferPool::new(65536 * 16, 8); // larger pool for multi
        let mut active = crate::segment::buffer::ActiveSegment::new(
            SizeTier::Multi,
            &SegmentSizeConfig { default_target_size: 10_485_760, ..SegmentSizeConfig::default() },
            &pool,
        )
        .unwrap();
        let key = ObjectKey::new("multi-test");
        // 3 MB, each chunk is 1 MB (default_target_size)
        let data = Bytes::from(vec![0xEE; 3_145_728]);

        let refs = route_write(&router, &metadata, &mut active, key, data).unwrap();

        // 3 MB / 1 MB = 3 chunks
        assert_eq!(refs.len(), 3, "3 MB blob should split into 3 chunks");
        assert_eq!(refs[0].offset, 0);
        assert_eq!(refs[0].length, 1_048_576);
        assert_eq!(refs[1].offset, 1_048_576);
        assert_eq!(refs[1].length, 1_048_576);
        assert_eq!(refs[2].offset, 2_097_152);
        assert_eq!(refs[2].length, 1_048_576);
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn route_write_empty_blob_returns_empty_refs() {
        let metadata = test_config();
        let router = TierRouter::new(SegmentSizeConfig::default());
        let pool = test_pool(65536, 8);
        let mut active = crate::segment::buffer::ActiveSegment::new(
            SizeTier::Standard,
            &SegmentSizeConfig::default(),
            &pool,
        )
        .unwrap();
        let key = ObjectKey::new("empty");

        let refs = route_write(&router, &metadata, &mut active, key, Bytes::new()).unwrap();
        assert!(refs.is_empty());
    }
}
