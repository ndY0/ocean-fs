//! Write-path orchestration — routes blob writes through the tier system.
//!
//! Contains `InlineWriter` for inline blob storage and the `route_write`
//! function that dispatches writes to the correct tier.

use bytes::Bytes;
use oceanfs_core::{ObjectKey, SizeTier};

use crate::{
    error::Result,
    metadata::MetadataStore,
    segment::{
        splitter::SegmentSplitter,
        tier::{ChunkListBuilder, TierRouter},
    },
};

/// Writes blobs directly to the metadata store (inline path).
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
        metadata: &MetadataStore,
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
#[allow(dead_code)]
pub(crate) fn route_write(
    router: &TierRouter,
    metadata: &MetadataStore,
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
            Ok(ChunkListBuilder::single(segment_id, offset, length as u32))
        }
        SizeTier::Multi => {
            let splitter = SegmentSplitter::new(router.target_size(SizeTier::Multi));
            let chunks = splitter.split(&data);
            let mut refs = Vec::with_capacity(chunks.len());
            for (chunk_offset, chunk_data) in &chunks {
                let segment_id = active.id();
                let (_offset, _length) = active.append(chunk_data)?;
                refs.push((segment_id, *chunk_offset, chunk_data.len() as u32));
            }
            Ok(ChunkListBuilder::multi(refs))
        }
        _ => Ok(smallvec::SmallVec::new()),
    }
}
